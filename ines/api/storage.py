# -*- coding: utf-8 -*-

import datetime
from io import BytesIO
from math import ceil
from os.path import basename
from os.path import isfile
from os.path import join as join_path
from tempfile import gettempdir

from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Unicode
from sqlalchemy.orm import aliased

from ines import OPEN_BLOCK_SIZE
from ines.api.database.sql import BaseSQLSession
from ines.api.database.sql import BaseSQLSessionManager
from ines.api.database.sql import new_lightweight_named_tuple
from ines.api.database.sql import sql_declarative_base
from ines.api.jobs import job
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import maybe_date
from ines.convert import maybe_integer
from ines.convert import maybe_set
from ines.convert import maybe_string
from ines.convert import maybe_unicode
from ines.exceptions import Error
from ines.mimetype import find_mimetype
from ines.utils import file_unique_code
from ines.utils import get_dir_filenames
from ines.utils import get_open_file
from ines.utils import make_unique_hash
from ines.utils import make_dir
from ines.utils import MissingDictList
from ines.utils import MissingList
from ines.utils import put_binary_on_file
from ines.utils import remove_file_quietly
from ines.utils import string_unique_code


TODAY_DATE = datetime.date.today
FilesDeclarative = sql_declarative_base('ines.storage')
FILES_TEMPORARY_DIR = join_path(gettempdir(), 'ines-tmp-files')


class BaseStorageSessionManager(BaseSQLSessionManager):
    __api_name__ = 'storage'
    __database_name__ = 'ines.storage'

    def __init__(self, *args, **kwargs):
        super(BaseStorageSessionManager, self).__init__(*args, **kwargs)
        make_dir(self.settings['path'])

        if issubclass(self.session, BaseStorageWithImageSession):
            from PIL import Image
            self.image_cls = Image
            self.resize_quality = Image.ANTIALIAS

            self.resizes = {}
            for key, value in self.settings.items():
                if key.startswith('resize.'):
                    blocks = key.split('.', 3)
                    if len(blocks) == 4:
                        resize_type, application_code, name = blocks[1:]
                        if resize_type in ('width', 'height'):
                            self.resizes.setdefault(application_code, {}).setdefault(name, {})[resize_type] = int(value)


class BaseStorageSession(BaseSQLSession):
    __api_name__ = 'storage'

    def save_file(
            self,
            binary,
            application_code,
            code_key=None,
            filename=None,
            title=None,
            parent_id=None,
            type_key=None):

        # Generate a unique code using SHA256
        if isinstance(binary, (bytes, basestring)):
            unique_code = string_unique_code(binary)
        else:
            unique_code = file_unique_code(binary)
            if not filename:
                filename = basename(binary.name)

        lock_key = u'storage save %s' % unique_code
        self.cache.lock(lock_key)
        try:
            file_path = (
                self.session
                .query(FilePath.id, FilePath.size, FilePath.mimetype)
                .filter(FilePath.code == unique_code)
                .first())

            # Save file if dont exists
            if not file_path:
                # Create a new filepath
                filename = maybe_string(filename)
                mimetype = find_mimetype(filename=filename, header_or_file=binary)
                file_path = FilePath(code=unique_code, mimetype=maybe_unicode(mimetype))

                to_add = []
                file_size = 0

                # Save blocks
                blocks = self.save_blocks(binary)
    
                for order, (block_id, block_size) in enumerate(blocks):
                    to_add.append(FileBlock(file_id_block=block_id, order=order))
                    file_size += block_size

                # Add file path to DB
                file_path.size = file_size
                self.session.add(file_path)
                self.session.flush()

                # Relate blocks and file path
                for block_relation in to_add:
                    block_relation.file_id_path = file_path.id
                self.session.add_all(to_add)
                self.session.flush()

            # Add applications file relation
            new = File(
                file_id=file_path.id,
                parent_id=parent_id,
                key=make_unique_hash(70),
                application_code=force_unicode(application_code),
                code_key=maybe_unicode(code_key),
                filename=maybe_unicode(filename),
                title=maybe_unicode(title),
                type_key=maybe_unicode(type_key))
            self.session.add(new)
            self.session.flush()

            # Add some attributes
            new.size = file_path.size
            new.mimetype = file_path.mimetype
            return new
        finally:
            self.cache.unlock(lock_key)

    @reify
    def block_size(self):
        min_size = 2**20
        size_block = int(self.settings.get('file_block_size') or min_size)
        if size_block < min_size:
            return min_size
        else:
            return size_block

    @reify
    def storage_path(self):
        return self.settings['path']

    @reify
    def max_blocks_per_folder(self):
        return int(self.settings.get('max_blocks_per_folder') or 250)

    def create_file_path(self, file_date=None):
        file_date = maybe_date(file_date or TODAY_DATE())
        base_folder_path = file_date.strftime('%Y%m/%d')

        last_folder = 0
        full_base_folder_path = join_path(self.storage_path, base_folder_path)
        folders = sorted(int(i) for i in get_dir_filenames(full_base_folder_path) if i.isdigit())
        if folders:
            last_folder = folders[-1]
        folder_path = join_path(base_folder_path, str(last_folder))

        full_folder_path = join_path(self.storage_path, folder_path)
        if len(get_dir_filenames(full_folder_path)) >= self.max_blocks_per_folder:
            folder_path = join_path(base_folder_path, str(last_folder + 1))

        while True:
            filename = make_unique_hash(length=80)
            path = join_path(folder_path, force_string(filename))
            full_path = join_path(self.storage_path, path)
            if not isfile(full_path):
                return full_path, path

    def save_blocks(self, binary):
        if isinstance(binary, (bytes, basestring)):
            binary_is_string = True
        else:
            binary.seek(0)
            binary_is_string = False

        blocks = []
        block_size = self.block_size
        while_binary = binary
        while True:
            if binary_is_string:
                block = while_binary[:block_size]
                while_binary = while_binary[block_size:]
            else:
                block = while_binary.read(block_size)
            if not block:
                break
            # Create hash of the block
            blocks.append(string_unique_code(block))
        if not blocks:
            raise ValueError('Empty file')

        # Lock all blocks
        locked_keys = dict((k, 'storage block save %s' % k) for k in set(blocks))
        for lock_key in locked_keys.values():
            self.cache.lock(lock_key)

        response = []
        try:
            # Look for existing blocks
            existing_blocks = {}
            for block in (
                    self.session
                    .query(BlockPath.id, BlockPath.size, BlockPath.code)
                    .filter(BlockPath.code.in_(set(blocks)))
                    .all()):
                existing_blocks[block.code] = (block.id, block.size)
                self.cache.unlock(locked_keys.pop(block.code))

            # Add missing blocks
            for order, block_hash in enumerate(blocks):
                if block_hash in existing_blocks:
                    response.append(existing_blocks[block_hash])
                else:
                    if binary_is_string:
                        block_binary = binary[order * block_size:block_size]
                    else:
                        binary.seek(order * block_size)
                        block_binary = binary.read(block_size)

                    full_path, path = self.create_file_path()
                    put_binary_on_file(full_path, block_binary, make_dir_recursively=True)

                    # Lets flush the session to prevent waiting in a possible locked block
                    block_size = len(block_binary)
                    block_id = (
                        self.direct_insert(BlockPath(path=path, size=block_size, code=block_hash))
                        .lastrowid)

                    response.append((block_id, block_size))
                    existing_blocks[block_hash] = (block_id, block_size)
                    self.cache.unlock(locked_keys.pop(block_hash))

        finally:
            for lock_key in locked_keys.values():
                self.cache.unlock(lock_key)

        return response

    def delete_files(self, *ids):
        if not ids:
            raise ValueError('Need to define some ids')

        # Get possible files to delete
        delete_file_ids = set()
        files_paths_ids = set()
        for f in self.session.query(File.id, File.file_id).filter(File.id.in_(ids)).all():
            files_paths_ids.add(f.file_id)
            delete_file_ids.add(f.id)
        if not delete_file_ids:
            return True

        # Check if we can delete some file paths
        delete_file_path_ids = files_paths_ids.difference(
            f.file_id
            for f in (
                self.session
                .query(File.file_id)
                .filter(File.file_id.in_(files_paths_ids))
                .filter(File.id.notin_(delete_file_ids))
                .all()))
        if delete_file_path_ids:
            # Get existing files blocks
            blocks_ids = set(
                f.file_id_block
                for f in (
                    self.session
                    .query(FileBlock.file_id_block)
                    .filter(FileBlock.file_id_path.in_(delete_file_path_ids))
                    .all()))

            # Check if we can delete some file block relations
            delete_block_ids = blocks_ids.difference(
                f.file_id_block
                for f in (
                    self.session
                    .query(FileBlock.file_id_block)
                    .filter(FileBlock.file_id_block.in_(blocks_ids))
                    .filter(FileBlock.file_id_path.notin_(delete_file_path_ids))
                    .all()))

            if delete_block_ids:
                # Get paths to delete
                delete_paths = set(
                    b.path
                    for b in (
                        self.session
                        .query(BlockPath.path)
                        .filter(BlockPath.id.in_(delete_block_ids))
                        .all()))

        # Delete files relations
        self.direct_delete(File, File.id.in_(delete_file_ids))
        if delete_file_path_ids:
            # Delete blocks relations
            self.direct_delete(FileBlock, FileBlock.file_id_path.in_(delete_file_path_ids))
            # Delete files paths from DB
            self.direct_delete(FilePath, FilePath.id.in_(delete_file_path_ids))
            if delete_block_ids:
                # Delete blocks paths from DB
                self.direct_delete(BlockPath, BlockPath.id.in_(delete_block_ids))

                # Delete blocks paths from storage
                for path in delete_paths:
                    remove_file_quietly(join_path(self.storage_path, path))

        return True

    def get_files(
            self,
            id=None,
            key=None,
            application_code=None,
            code_key=None,
            type_key=None,
            parent_key=None,
            attributes=None,
            only_one=False):

        attributes = set(attributes or ['id'])
        return_open_file = 'open_file' in attributes
        if return_open_file:
            attributes.remove('open_file')
            attributes.add('file_id')

        columns = []
        relate_with_path = False
        for attribute in attributes:
            if attribute in File.__table__.c:
                columns.append(File.__table__.c[attribute])
            if attribute not in ('id', 'created_date') and attribute in FilePath.__table__.c:
                columns.append(FilePath.__table__.c[attribute])
                relate_with_path = True
        query = self.session.query(*columns or [File.id])

        if relate_with_path:
            query = query.filter(File.file_id == FilePath.id)

        if id:
            query = query.filter(File.id.in_(maybe_set(id)))
        if key:
            query = query.filter(File.key.in_(maybe_set(key)))
        if application_code:
            query = query.filter(File.application_code == force_unicode(application_code))
        if code_key:
            query = query.filter(File.code_key == force_unicode(code_key))
        if type_key:
            query = query.filter(File.type_key == force_unicode(type_key))
        if parent_key:
            parent = aliased(File)
            query = query.filter(File.parent_id == parent.id).filter(parent.key == force_unicode(parent_key))

        if only_one:
            response = query.first()
        else:
            response = query.all()

        if not return_open_file or not response:
            return response

        if only_one:
            response = [response]

        files_blocks = MissingList()
        for block in (
                self.session
                .query(BlockPath.path, FileBlock.file_id_path)
                .filter(BlockPath.id == FileBlock.file_id_block)
                .filter(FileBlock.file_id_path.in_(set(f.file_id for f in response)))
                .order_by(FileBlock.order)
                .all()):
            files_blocks[block.file_id_path].append(block.path)

        # Add items to SQLAlchemy tuple result
        new_namedtuple = new_lightweight_named_tuple(response[0], 'open_file')
        response[:] = [
            new_namedtuple(r + (StorageFile(self.storage_path, files_blocks[r.file_id]), ))
            for r in response]

        if only_one:
            return response[0]
        else:
            return response

    def get_file(self, **kwargs):
        return self.get_files(only_one=True, **kwargs)


class BaseStorageWithImageSession(BaseStorageSession):
    def verify_image(self, binary_or_file):
        if isinstance(binary_or_file, (bytes, basestring)):
            binary_or_file = BytesIO(binary_or_file)

        try:
            im = self.api_session_manager.image_cls.open(binary_or_file)
            im.verify()
        except:
            raise Error('image', 'Invalid image.')
        else:
            return im

    def get_thumbnail(self, key, resize_name, attributes=None):
        resized = self.get_files(
            parent_key=key,
            type_key=u'resize-%s' % resize_name,
            only_one=True,
            attributes=attributes)
        if resized:
            return resized

        f = self.session.query(File.id, File.application_code).filter(File.key == key).first()
        if (f
                and f.application_code in self.api_session_manager.resizes
                and resize_name in self.api_session_manager.resizes[f.application_code]
                and self.resize_image(f.id, f.application_code, resize_name)):
            # Thumb created on the fly
            self.flush()
            return self.get_files(
                parent_key=key,
                type_key=u'resize-%s' % resize_name,
                only_one=True,
                attributes=attributes)

    def resize_image(self, file_id, application_code, resize_name):
        if application_code not in self.api_session_manager.resizes:
            raise Error('application_code', u'Invalid application code: %s' % application_code)

        resize = self.api_session_manager.resizes[application_code].get(resize_name)
        if not resize:
            raise Error('resize_name', u'Invalid resize name: %s' % resize_name)
        resize_width = maybe_integer(resize.get('width'))
        resize_height = maybe_integer(resize.get('height'))
        if not resize_width and not resize_height:
            raise Error('resize', u'Invalid resize options')

        file_info = self.get_file(
            id=file_id,
            application_code=application_code,
            attributes=['open_file', 'key', 'filename', 'title', 'code_key'])
        if not file_info:
            raise Error('file', u'File ID not found')

        temporary_path = None
        type_key = u'resize-%s' % resize_name
        filename = None
        if file_info.filename:
            filename = u'%s-%s' % (resize_name, file_info.filename)

        lock_key = 'create image resize %s %s' % (file_id, resize_name)
        self.cache.lock(lock_key)
        try:
            existing = (
                self.session
                .query(File)
                .filter(File.parent_id == file_id)
                .filter(File.application_code == application_code)
                .filter(File.type_key == type_key)
                .first())

            if not existing:
                im = self.api_session_manager.image_cls.open(file_info.open_file)

                width = int(im.size[0])
                height = int(im.size[1])

                if not resize_width:
                    resize_width = ceil((float(width) * resize_height) / height)

                elif not resize_height:
                    resize_height = ceil((float(resize_width) * height) / width)

                else:
                    resize_racio = resize_width / float(resize_height)
                    image_racio = width / float(height)

                    if image_racio < resize_racio:
                        # Crop image on height
                        crop_size = ceil(round(height - (width / resize_racio)) / 2)
                        lower_position = int(height - int(crop_size))
                        # Crop as left, upper, right, and lower pixel
                        im = im.crop((0, int(crop_size), width, lower_position))

                    elif image_racio > resize_racio:
                        crop_size = ceil(round(width - (height * resize_racio)) / 2)
                        right_position = int(width - int(crop_size))
                        # Crop as left, upper, right, and lower pixel
                        im = im.crop((int(crop_size), 0, right_position, height))

                # Resize image
                im = im.resize((int(resize_width), int(resize_height)), self.api_session_manager.resize_quality)
                temporary_path = save_temporary_image(im)
                resized = get_open_file(temporary_path)
                self.save_file(
                    resized,
                    application_code=application_code,
                    code_key=file_info.code_key,
                    type_key=type_key,
                    filename=filename,
                    title=file_info.title,
                    parent_id=file_id)

        finally:
            self.cache.unlock(lock_key)

            if temporary_path:
                remove_file_quietly(temporary_path)

        return True

    @job(second=0, minute=[0, 30])
    def create_image_resizes(self):
        if not asbool(self.settings.get('thumb.create_on_add')) or not self.api_session_manager.resizes:
            return None

        existing = MissingDictList()
        for t in (
                self.session
                .query(File.parent_id, File.type_key, File.application_code)
                .filter(File.application_code.in_(self.api_session_manager.resizes.keys()))
                .filter(File.parent_id.isnot(None))
                .filter(File.type_key.like(u'thumb-%'))
                .all()):
            existing[t.application_code][t.parent_id].append(t.type_key.replace(u'thumb-', u'', 1))

        files = MissingList()
        for f in (
                self.session
                .query(File.id, File.application_code)
                .filter(File.parent_id.is_(None))
                .filter(File.application_code.in_(self.api_session_manager.resizes.keys()))
                .all()):
            files[f.application_code].append(f.id)

        for application_code, resize_names in self.api_session_manager.resizes.items():
            app_files = files[application_code]
            if not app_files:
                continue

            existing_app_names = existing[application_code]
            for resize_name in resize_names.keys():
                for f in app_files:
                    if f not in existing_app_names or resize_name not in existing_app_names[f]:
                        self.resize_image(f, application_code, resize_name)
                        self.flush()

    def save_image(
            self,
            binary,
            application_code,
            code_key=None,
            filename=None,
            title=None,
            crop_left=None,
            crop_upper=None,
            crop_right=None,
            crop_lower=None,
            image_validation=None):

        im = self.verify_image(binary)
        if image_validation:
            image_validation(im)

        temporary_path = None
        if crop_left or crop_upper or crop_right or crop_lower:
            crop_left = int(crop_left or 0)
            crop_upper = int(crop_upper or 0)
            crop_right = int(crop_right or 0)
            crop_lower = int(crop_lower or 0)
            width = int(im.size[0])
            height = int(im.size[1])

            if (crop_left + crop_right) >= width:
                raise Error('crop_left+crop_right', u'Left and right crop bigger then image width')
            elif (crop_upper + crop_lower) >= height:
                raise Error('crop_upper+crop_lower', u'Upper and lower crop bigger then image height')

            new_im = self.api_session_manager.image_cls.open(binary)
            new_im = new_im.crop((int(crop_left), int(crop_upper), int(width - crop_right), int(height - crop_lower)))
            temporary_path = save_temporary_image(new_im)
            binary = get_open_file(temporary_path)

        f = self.save_file(binary, application_code, code_key, filename=filename, title=title)
        if asbool(self.settings.get('thumb.create_on_add')):
            self.create_image_resizes.run_job()

        if temporary_path:
            remove_file_quietly(temporary_path)

        return f

    def delete_images(self, *ids):
        children = set(f.id for f in self.session.query(File.id).filter(File.parent_id.in_(ids)).all())
        if children:
            self.delete_images(*children)  # Recursively
        return self.delete_files(*ids)


class FilePath(FilesDeclarative):
    __tablename__ = 'storage_file_paths'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(Unicode(70), unique=True, nullable=False)
    size = Column(Integer, nullable=False)
    mimetype = Column(Unicode(100))
    created_date = Column(DateTime, nullable=False, default=func.now())


class File(FilesDeclarative):
    __tablename__ = 'storage_files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey(FilePath.id), nullable=False)
    parent_id = Column(Integer, ForeignKey('storage_files.id'))
    key = Column(Unicode(70), unique=True, nullable=False)
    application_code = Column(Unicode(50), nullable=False)
    code_key = Column(Unicode(150))
    type_key = Column(Unicode(50))
    filename = Column(Unicode(255))
    title = Column(Unicode(255))
    created_date = Column(DateTime, nullable=False, default=func.now())

Index('storage_file_code_idx', File.application_code, File.code_key)


class BlockPath(FilesDeclarative):
    __tablename__ = 'storage_blocks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(Unicode(70), unique=True, nullable=False)
    path = Column(String(255), nullable=False)
    size = Column(Integer, nullable=False)
    created_date = Column(DateTime, nullable=False, default=func.now())


class FileBlock(FilesDeclarative):
    __tablename__ = 'storage_file_blocks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id_path = Column(Integer, ForeignKey(FilePath.id), nullable=False)
    file_id_block = Column(Integer, ForeignKey(BlockPath.id), nullable=False)
    order = Column(Integer, nullable=False)


class StorageFile(object):
    def __init__(self, storage_path, blocks, block_size=OPEN_BLOCK_SIZE):
        self.storage_path = storage_path
        self.blocks = blocks
        self.block_position = 0
        self.block_size = block_size

    def read(self, size=-1):
        if size == 0:
            return b''

        try:
            open_block = self.blocks[self.block_position]
        except IndexError:
            return b''

        if isinstance(open_block, basestring):
            open_block = self.blocks[self.block_position] = get_open_file(join_path(self.storage_path, open_block))

        binary = open_block.read(size)
        if size > 0:
            size -= len(binary)
            if size <= 0:
                return binary

        self.block_position += 1
        binary += self.read(size)
        return binary

    def __iter__(self):
        return self

    def next(self):
        val = self.read(self.block_size)
        if not val:
            raise StopIteration
        return val

    __next__ = next # py3

    def close(self):
        for block in self.blocks:
            if not isinstance(block, basestring):
                block.close()


def save_temporary_image(im, default_format='JPEG'):
    temporary_path = join_path(FILES_TEMPORARY_DIR, make_unique_hash(64))
    open_file = get_open_file(temporary_path, mode='wb')
    im.save(open_file, format=im.format or default_format)
    return temporary_path
