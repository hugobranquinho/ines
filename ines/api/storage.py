# -*- coding: utf-8 -*-

from base64 import b64encode
from collections import defaultdict
from io import BytesIO
from math import ceil
from os.path import basename, isfile, join as join_paths
from tempfile import gettempdir

from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import Column, DateTime, ForeignKey, func, Index, Integer, String, Unicode, UnicodeText
from sqlalchemy.orm import aliased

from ines import lazy_import_module, OPEN_BLOCK_SIZE, TODAY_DATE
from ines.api.database.sql import (
    BaseSQLSession, BaseSQLSessionManager, new_lightweight_named_tuple, sql_declarative_base)
from ines.api.jobs import job
from ines.convert import maybe_date, maybe_integer, maybe_set, maybe_string, to_bytes, to_string
from ines.exceptions import Error
from ines.i18n import _
from ines.mimetype import find_mimetype
from ines.url import get_url_file, open_json_url
from ines.utils import (
    file_unique_code, get_dir_filenames, get_open_file, make_unique_hash, make_dir, put_binary_on_file,
    remove_file_quietly, string_unique_code)


FilesDeclarative = sql_declarative_base('ines.storage')
FILES_TEMPORARY_DIR = join_paths(gettempdir(), 'ines-tmp-files')


class BaseStorageSessionManager(BaseSQLSessionManager):
    __api_name__ = 'storage'
    __database_name__ = 'ines.storage'

    def __init__(self, *args, **kwargs):
        super(BaseStorageSessionManager, self).__init__(*args, **kwargs)
        make_dir(self.settings['folder_path'])

        if issubclass(self.session, BaseStorageWithImageSession):
            self.image_cls = lazy_import_module('PIL.Image')
            self.resize_quality = self.image_cls.ANTIALIAS

            self.resizes = {}
            for key, value in self.settings.items():
                if key.startswith('resize.'):
                    blocks = key.split('.', 3)
                    if len(blocks) == 4:
                        resize_type, application_code, name = blocks[1:]
                        if resize_type in ('width', 'height'):
                            self.resizes.setdefault(application_code, {}).setdefault(name, {})[resize_type] = int(value)

            self.tinypng_api = self.settings.get('tinypng_api')
            self.tinypng_locked_months = []


class BaseStorageSession(BaseSQLSession):
    __api_name__ = 'storage'

    def save_file_path(self, binary, filename=None, compressed=False):
        # Generate a unique code using SHA256
        if isinstance(binary, StorageFile):
            filename = filename or binary.name
            binary = binary.read()
        if isinstance(binary, (bytes, str)):
            unique_code = string_unique_code(binary)
        else:
            unique_code = file_unique_code(binary)
            if not filename:
                filename = basename(binary.name)

        lock_key = 'storage save %s' % unique_code
        self.cache.lock(lock_key)
        try:
            file_path = (
                self.session
                .query(*FilePath.__table__.c.values())
                .filter(FilePath.code == unique_code)
                .first())

            # Save file if dont exists
            if not file_path:
                # Create a new filepath
                filename = maybe_string(filename)
                mimetype = find_mimetype(filename=filename, header_or_file=binary)
                file_path = FilePath(code=unique_code, mimetype=maybe_string(mimetype), compressed=compressed)

                to_add = []
                file_size = 0

                # Save blocks
                blocks = self.save_blocks(binary)

                for order, (block_id, block_size) in enumerate(blocks):
                    to_add.append(FileBlock(file_id_block=block_id, order=order))
                    file_size += block_size

                # Add file path to DB
                file_path.size = file_size
                file_path.id = self.direct_insert(file_path).inserted_primary_key[0]

                # Relate blocks and file path
                for block_relation in to_add:
                    block_relation.file_id_path = file_path.id
                    self.direct_insert(block_relation)

            return file_path
        finally:
            self.cache.unlock(lock_key)

    def save_file(
            self,
            binary,
            application_code,
            code_key=None,
            type_key=None,
            data=None,
            filename=None,
            title=None,
            parent_id=None):

        file_path = self.save_file_path(binary, filename)

        session_id = session_type = None
        if self.request.authenticated:
            session_id = self.request.authenticated.session_id
            session_type = self.request.authenticated.session_type

        # Add applications file relation
        new = File(
            file_id=file_path.id,
            parent_id=parent_id,
            key=make_unique_hash(70),
            application_code=to_string(application_code),
            code_key=maybe_string(code_key),
            type_key=maybe_string(type_key),
            data=maybe_string(data),
            filename=maybe_string(filename),
            title=maybe_string(title),
            session_type=session_type,
            session_id=session_id)
        self.session.add(new)
        self.session.flush()

        # Add some attributes
        new.size = file_path.size
        new.mimetype = file_path.mimetype
        return new

    @reify
    def block_size(self):
        return int(self.settings.get('file_block_size') or OPEN_BLOCK_SIZE)

    @reify
    def storage_path(self):
        return self.settings['folder_path']

    @reify
    def max_blocks_per_folder(self):
        return int(self.settings.get('max_blocks_per_folder') or 250)

    def create_file_path(self, file_date=None):
        file_date = maybe_date(file_date or TODAY_DATE())
        base_folder_path = file_date.strftime('%Y%m/%d')

        last_folder = 0
        full_base_folder_path = join_paths(self.storage_path, base_folder_path)
        folders = sorted(int(i) for i in get_dir_filenames(full_base_folder_path) if i.isdigit())
        if folders:
            last_folder = folders[-1]
        folder_path = join_paths(base_folder_path, last_folder)

        full_folder_path = join_paths(self.storage_path, folder_path)
        if len(get_dir_filenames(full_folder_path)) >= self.max_blocks_per_folder:
            folder_path = join_paths(base_folder_path, last_folder + 1)

        while True:
            filename = make_unique_hash(length=80)
            path = join_paths(folder_path, filename)
            full_path = join_paths(self.storage_path, path)
            if not isfile(full_path):
                return full_path, path

    def save_blocks(self, binary):
        if isinstance(binary, (bytes, str)):
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
        locked_keys = {k: 'storage block save %s' % k for k in set(blocks)}
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
                        start_idx = order * block_size
                        block_binary = binary[start_idx:start_idx + block_size]
                    else:
                        binary.seek(order * block_size)
                        block_binary = binary.read(block_size)

                    full_path, path = self.create_file_path()
                    put_binary_on_file(full_path, block_binary, make_dir_recursively=True)

                    # Lets flush the session to prevent waiting in a possible locked block
                    block_size = len(block_binary)
                    block_response = self.direct_insert(BlockPath(path=path, size=block_size, code=block_hash))
                    block_id = block_response.inserted_primary_key[0]

                    response.append((block_id, block_size))
                    existing_blocks[block_hash] = (block_id, block_size)
                    self.cache.unlock(locked_keys.pop(block_hash))

        finally:
            for lock_key in locked_keys.values():
                self.cache.unlock(lock_key)

        return response

    def delete_file_paths(self, *ids):
        if not ids:
            return False

        ids = maybe_set(ids)

        # Get existing files blocks
        blocks_ids = set(
            f.file_id_block
            for f in (
                self.session
                .query(FileBlock.file_id_block)
                .filter(FileBlock.file_id_path.in_(ids))
                .all()))

        # Check if we can delete some file block relations
        delete_block_ids = blocks_ids.difference(
            f.file_id_block
            for f in (
                self.session
                .query(FileBlock.file_id_block)
                .filter(FileBlock.file_id_block.in_(blocks_ids))
                .filter(FileBlock.file_id_path.notin_(ids))
                .all()))

        delete_paths = None
        if delete_block_ids:
            # Get paths to delete
            delete_paths = set(
                b.path
                for b in (
                    self.session
                    .query(BlockPath.path)
                    .filter(BlockPath.id.in_(delete_block_ids))
                    .all()))

        # Delete blocks relations
        self.direct_delete(FileBlock, FileBlock.file_id_path.in_(ids))
        # Delete files paths from DB
        self.direct_delete(FilePath, FilePath.id.in_(ids))

        if delete_block_ids:
            # Delete blocks paths from DB
            self.direct_delete(BlockPath, BlockPath.id.in_(delete_block_ids))

            # Delete blocks paths from storage
            for path in delete_paths:
                remove_file_quietly(join_paths(self.storage_path, path))

        return True

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

        # Delete files relations
        self.direct_delete(File, File.id.in_(delete_file_ids))

        if delete_file_path_ids:
            self.delete_file_paths(*delete_file_path_ids)

        return True

    def get_files(
            self,
            key=None,
            attributes=None,
            only_one=False,
            **kwargs):

        attributes = set(attributes or ['id'])
        return_open_file = 'open_file' in attributes
        if return_open_file:
            attributes.remove('open_file')
            attributes.add('id')

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

        if 'id' in kwargs:
            query = query.filter(File.id.in_(maybe_set(kwargs['id'])))
        if key:
            query = query.filter(File.key.in_(maybe_set(key)))
        if 'file_id' in kwargs:
            query = query.filter(File.file_id.in_(maybe_set(kwargs['file_id'])))

        if 'application_code' in kwargs:
            application_code = kwargs['application_code']
            if application_code is None:
                query = query.filter(File.application_code.is_(None))
            else:
                query = query.filter(File.application_code == to_string(application_code))

        if 'code_key' in kwargs:
            code_key = kwargs['code_key']
            if code_key is None:
                query = query.filter(File.code_key.is_(None))
            else:
                query = query.filter(File.code_key == to_string(code_key))

        if 'type_key' in kwargs:
            type_key = kwargs['type_key']
            if type_key is None:
                query = query.filter(File.type_key.is_(None))
            else:
                query = query.filter(File.type_key == to_string(type_key))

        if 'data' in kwargs:
            data = kwargs['data']
            if data is None:
                query = query.filter(File.data.is_(None))
            else:
                query = query.filter(File.data == to_string(data))

        if 'parent_id' in kwargs:
            parent_id = kwargs['parent_id']
            if parent_id is None:
                query = query.filter(File.parent_id.is_(None))
            else:
                query = query.filter(File.parent_id == int(parent_id))

        parent_key = kwargs.get('parent_key')
        if parent_key:
            parent = aliased(File)
            query = query.filter(File.parent_id == parent.id).filter(parent.key == to_string(parent_key))

        order_by = kwargs.get('order_by')
        if order_by is not None:
            query = query.order_by(order_by)

        if only_one:
            response = query.first()
        else:
            response = query.all()

        if not return_open_file or not response:
            return response

        if only_one:
            response = [response]

        # Add items to SQLAlchemy tuple result
        files_binary = self.get_files_binary(*(f.id for f in response))
        new_namedtuple = new_lightweight_named_tuple(response[0], 'open_file')
        response[:] = [
            new_namedtuple(r + (files_binary[r.id], ))
            for r in response]

        if only_one:
            return response[0]
        else:
            return response

    def get_files_binary(self, *file_ids):
        filenames = {}
        file_ids = set(file_ids)
        files_blocks = defaultdict(list)

        for block in (
                self.session
                .query(File.id, File.filename, BlockPath.path)
                .filter(BlockPath.id == FileBlock.file_id_block)
                .filter(File.file_id == FileBlock.file_id_path)
                .filter(File.id.in_(file_ids))
                .order_by(FileBlock.order)
                .all()):
            files_blocks[block.id].append(block.path)
            filenames[block.id] = block.filename

        return dict(
            (i, StorageFile(self.storage_path, filenames.get(i), files_blocks[i]))
            for i in file_ids)

    def get_file_binary(self, file_id):
        return self.get_files_binary(file_id).get(file_id)

    def get_file(self, **kwargs):
        return self.get_files(only_one=True, **kwargs)


class BaseStorageWithImageSession(BaseStorageSession):
    def verify_image(self, binary_or_file):
        if isinstance(binary_or_file, (bytes, str)):
            binary_or_file = BytesIO(binary_or_file)

        try:
            im = self.api_session_manager.image_cls.open(binary_or_file)
            im.verify()
            im.close()
        except:
            raise Error('image', 'Invalid image.')
        else:
            return self.api_session_manager.image_cls.open(binary_or_file)

    def get_thumbnail(self, key, resize_name, attributes=None):
        resized = self.get_files(
            parent_key=key,
            type_key='resize-%s' % resize_name,
            only_one=True,
            attributes=attributes)
        if resized:
            return resized

        f = self.session.query(File.id, File.application_code).filter(File.key == key).first()
        if (f and resize_name in self.api_session_manager.resizes.get(f.application_code, [])
              and self.resize_image(f.id, f.application_code, resize_name)):
            # Thumb created on the fly
            self.flush()
            return self.get_files(
                parent_key=key,
                type_key='resize-%s' % resize_name,
                only_one=True,
                attributes=attributes)

    def resize_image(self, fid, application_code, resize_name):
        if application_code not in self.api_session_manager.resizes:
            raise Error('application_code', 'Invalid application code: %s' % application_code)

        resize = self.api_session_manager.resizes[application_code].get(resize_name)
        if not resize:
            raise Error('resize_name', 'Invalid resize name: %s' % resize_name)
        resize_width = maybe_integer(resize.get('width'))
        resize_height = maybe_integer(resize.get('height'))
        if not resize_width and not resize_height:
            raise Error('resize', 'Invalid resize options')

        file_info = self.get_file(
            id=fid,
            application_code=application_code,
            attributes=['open_file', 'key', 'filename', 'title', 'code_key'])
        if not file_info:
            raise Error('file', 'File ID not found')

        temporary_path = None
        type_key = 'resize-%s' % resize_name
        filename = None
        if file_info.filename:
            filename = '%s-%s' % (resize_name, file_info.filename)

        lock_key = 'create image resize %s %s' % (fid, resize_name)
        self.cache.lock(lock_key)
        try:
            existing = (
                self.session
                .query(File)
                .filter(File.parent_id == fid)
                .filter(File.application_code == application_code)
                .filter(File.type_key == type_key)
                .first())

            if not existing:
                original_temporary_path, original_file = create_temporary_file(mode='wb')
                original_file.write(file_info.open_file.read())
                original_file.close()

                original_file = get_open_file(original_temporary_path)
                im = self.api_session_manager.image_cls.open(original_file)

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

                original_file.close()
                remove_file_quietly(original_temporary_path)

                self.save_file(
                    resized,
                    application_code=application_code,
                    code_key=file_info.code_key,
                    type_key=type_key,
                    filename=filename,
                    title=file_info.title,
                    parent_id=fid)

        finally:
            self.cache.unlock(lock_key)

            if temporary_path:
                remove_file_quietly(temporary_path)

        return True

    @job(second=0, minute=[0, 30],
         title=_('Create images'),
         unique_name='ines:create_image_resizes')
    def create_image_resizes(self):
        if not asbool(self.settings.get('thumb.create_on_add')) or not self.api_session_manager.resizes:
            return None

        existing = defaultdict(lambda: defaultdict(list))
        for t in (
                self.session
                .query(File.parent_id, File.type_key, File.application_code)
                .filter(File.application_code.in_(self.api_session_manager.resizes.keys()))
                .filter(File.parent_id.isnot(None))
                .filter(File.type_key.like('thumb-%'))
                .all()):
            existing[t.application_code][t.parent_id].append(t.type_key.replace('thumb-', '', 1))

        files = defaultdict(list)
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

    @job(second=0, minute=15, hour=2,
         title=_('Compress images'),
         unique_name='ines:compress_images')
    def compress_images(self):
        month_str = TODAY_DATE().strftime('%Y%m')
        if not self.api_session_manager.tinypng_api or month_str in self.api_session_manager.tinypng_locked_months:
            return None

        headers = {
            'Authorization': b'Basic ' + b64encode(b'api:' + to_bytes(self.api_session_manager.tinypng_api))}

        while True:
            file_info = (
                self.session
                .query(File.id, File.file_id, File.filename)
                .filter(FilePath.compressed.is_(None))
                .filter(FilePath.mimetype.in_(['image/jpeg', 'image/png']))
                .filter(FilePath.id == File.file_id)
                .order_by(FilePath.size, File.filename.desc())
                .first())

            if not file_info:
                break

            binary = self.get_file_binary(file_info.id).read()
            response = open_json_url(
                'https://api.tinify.com/shrink',
                method='POST',
                data=binary,
                headers=headers)

            self.api_session_manager.tinypng_locked_months.append(month_str)

            if response['output']['ratio'] >= 1:
                self.direct_update(FilePath, FilePath.id == file_info.file_id, {'compressed': True})
            else:
                new_file_binary = get_url_file(response['output']['url'], headers=headers)
                file_path_id = self.save_file_path(new_file_binary, filename=file_info.filename, compressed=True).id
                if file_path_id == file_info.file_id:
                    self.direct_update(FilePath, FilePath.id == file_info.file_id, {'compressed': True})
                else:
                    self.direct_update(File, File.file_id == file_info.file_id, {'file_id': file_path_id})
                    self.delete_file_paths(file_info.file_id)

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
            image_validation=None,
            type_key=None,
        ):

        im = self.verify_image(binary)
        if image_validation:
            image_validation(im)

        if crop_left or crop_upper or crop_right or crop_lower:
            crop_left = int(crop_left or 0)
            crop_upper = int(crop_upper or 0)
            crop_right = int(crop_right or 0)
            crop_lower = int(crop_lower or 0)
            width = int(im.size[0])
            height = int(im.size[1])

            if (crop_left + crop_right) >= width:
                raise Error('crop_left+crop_right', 'Left and right crop bigger then image width')
            elif (crop_upper + crop_lower) >= height:
                raise Error('crop_upper+crop_lower', 'Upper and lower crop bigger then image height')

            im = im.crop((int(crop_left), int(crop_upper), int(width - crop_right), int(height - crop_lower)))

        temporary_path = save_temporary_image(im)
        binary = get_open_file(temporary_path)

        f = self.save_file(
            binary,
            application_code,
            code_key,
            type_key=type_key,
            filename=filename,
            title=title,
        )

        if asbool(self.settings.get('thumb.create_on_add')):
            self.create_image_resizes.run_job()
        if asbool(self.settings.get('compress_on_add')):
            self.compress_images.run_job()

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

    id = Column(Integer, primary_key=True)
    code = Column(Unicode(120), unique=True, nullable=False)
    compressed_code = Column(Unicode(120))

    size = Column(Integer, nullable=False)
    mimetype = Column(Unicode(100))
    created_date = Column(DateTime, nullable=False, default=func.now())


class File(FilesDeclarative):
    __tablename__ = 'storage_files'

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey(FilePath.id), nullable=False)
    parent_id = Column(Integer, ForeignKey('storage_files.id'))

    key = Column(Unicode(70), unique=True, nullable=False)
    application_code = Column(Unicode(50), nullable=False)
    code_key = Column(Unicode(150))
    type_key = Column(Unicode(50))
    data = Column(UnicodeText)
    filename = Column(Unicode(255))
    title = Column(Unicode(255))

    session_type = Column(Unicode(25))
    session_id = Column(Unicode(100))

    created_date = Column(DateTime, nullable=False, default=func.now())

Index('storage_file_code_idx', File.application_code, File.code_key)
Index('storage_code_key_idx', File.code_key)


class BlockPath(FilesDeclarative):
    __tablename__ = 'storage_blocks'

    id = Column(Integer, primary_key=True)
    code = Column(Unicode(120), unique=True, nullable=False)
    path = Column(String(255), nullable=False)
    size = Column(Integer, nullable=False)
    created_date = Column(DateTime, nullable=False, default=func.now())


class FileBlock(FilesDeclarative):
    __tablename__ = 'storage_file_blocks'

    id = Column(Integer, primary_key=True)
    file_id_path = Column(Integer, ForeignKey(FilePath.id), nullable=False)
    file_id_block = Column(Integer, ForeignKey(BlockPath.id), nullable=False)
    order = Column(Integer, nullable=False)


class StorageFile(object):
    def __init__(self, storage_path, name, blocks, block_size=OPEN_BLOCK_SIZE):
        self.storage_path = storage_path
        self.name = name
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

        if isinstance(open_block, str):
            open_block = self.blocks[self.block_position] = get_open_file(join_paths(self.storage_path, open_block))

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

    __next__ = next  # py3

    def close(self):
        for block in self.blocks:
            if not isinstance(block, str):
                block.close()

    def seek(self, position):
        # @@TODO
        self.block_position = 0
        for block in self.blocks:
            if not isinstance(block, str):
                block.seek(0)


def create_temporary_file(mode='wb'):
    temporary_path = join_paths(FILES_TEMPORARY_DIR, make_unique_hash(64))
    open_file = get_open_file(temporary_path, mode=mode)
    return temporary_path, open_file


def save_temporary_image(im, default_format='JPEG', mode='wb'):
    temporary_path, open_file = create_temporary_file(mode)

    try:
        im.save(open_file, format=im.format or default_format, optimize=True)
    except IOError as error:
        if error.args[0] == 'cannot write mode P as JPEG':
            im.convert('RGB').save(open_file, format=im.format or default_format, optimize=True)
        else:
            raise

    return temporary_path
