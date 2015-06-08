# -*- coding: utf-8 -*-

import datetime
from os.path import isfile
from os.path import join as join_path

from pyramid.decorator import reify
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Unicode

from ines.api.database.sql import BaseSQLSession
from ines.api.database.sql import BaseSQLSessionManager
from ines.api.database.sql import sql_declarative_base
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import maybe_date
from ines.convert import maybe_set
from ines.convert import maybe_string
from ines.convert import maybe_unicode
from ines.mimetype import find_mimetype
from ines.utils import file_unique_code
from ines.utils import get_dir_filenames
from ines.utils import get_open_file
from ines.utils import make_unique_hash
from ines.utils import make_dir
from ines.utils import MissingList
from ines.utils import put_binary_on_file
from ines.utils import remove_file_quietly
from ines.utils import string_unique_code


TODAY_DATE = datetime.date.today
FilesDeclarative = sql_declarative_base('ines.storage')


class BaseStorageSessionManager(BaseSQLSessionManager):
    __api_name__ = 'storage'
    __database_name__ = 'ines.storage'

    def __init__(self, *args, **kwargs):
        super(BaseStorageSessionManager, self).__init__(*args, **kwargs)
        make_dir(self.config.settings['%s.path' % self.__api_name__])


class BaseStorageSession(BaseSQLSession):
    __api_name__ = 'storage'

    def save_file(
            self,
            binary,
            application_code,
            code_key,
            filename=None,
            title=None):

        # Generate a unique code using SHA256
        if isinstance(binary, file):
            unique_code = file_unique_code(binary)
        else:
            unique_code = string_unique_code(binary)

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
                mimetype = find_mimetype(filename=filename, binary=binary)
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
                key=make_unique_hash(70),
                application_code=force_unicode(application_code),
                code_key=force_unicode(code_key),
                filename=maybe_unicode(filename),
                title=maybe_unicode(title))
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
        size_block = int(self.settings.get('%s.file_block_size' % self.__api_name__) or min_size)
        if size_block < min_size:
            return min_size
        else:
            return size_block

    @reify
    def storage_path(self):
        return self.settings['%s.path' % self.__api_name__]

    @reify
    def max_blocks_per_folder(self):
        return int(self.settings.get('%s.max_blocks_per_folder' % self.__api_name__) or 250)

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
        if isinstance(binary, file):
            binary.seek(0)
            binary_is_string = False
        else:
            binary_is_string = True

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

        # Delete files relations
        self.direct_delete(File, File.id.in_(delete_file_ids))

        # Check if we can delete some file paths
        delete_file_path_ids = files_paths_ids.difference(
            f.file_id
            for f in (
                self.session
                .query(File.file_id)
                .filter(File.file_id.in_(files_paths_ids))
                .all()))
        if not delete_file_path_ids:
            return True

        # Get existing files blocks
        blocks_ids = set(
            f.file_id_block
            for f in (
                self.session
                .query(FileBlock.file_id_block)
                .filter(FileBlock.file_id_path.in_(delete_file_path_ids))
                .all()))

        # Delete blocks relations
        self.direct_delete(FileBlock, FileBlock.file_id_path.in_(delete_file_path_ids))
        # Delete files paths from DB
        self.direct_delete(FilePath, FilePath.id.in_(delete_file_path_ids))

        # Check if we can delete some file block relations
        delete_block_ids = blocks_ids.difference(
            f.file_id_block
            for f in (
                self.session
                .query(FileBlock.file_id_block)
                .filter(FileBlock.file_id_block.in_(blocks_ids))
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
            attributes=None):

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
            if attribute in FilePath.__table__.c:
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
            query = query.filter(File.application_code == application_code)
        if code_key:
            query = query.filter(File.code_key == code_key)

        response = query.all()
        if not return_open_file or not response:
            return response

        files_blocks = MissingList()
        for block in (
                self.session
                .query(BlockPath.size, BlockPath.path, FileBlock.file_id_path)
                .filter(BlockPath.id == FileBlock.file_id_block)
                .filter(FileBlock.file_id_path.in_(set(f.file_id for f in response)))
                .order_by(FileBlock.order)
                .all()):
            files_blocks[block.file_id_path].append((block.size, block.path))

        for f in response:
            f.open_file = StorageFile(self.storage_path, files_blocks[f.file_id])

        return response


class BaseStorageWithImageSession(BaseStorageSession):
    def save_image(self, *args, **kwargs):
        return self.save_file(*args, **kwargs)


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
    key = Column(Unicode(70), unique=True, nullable=False)
    application_code = Column(Unicode(50), nullable=False)
    code_key = Column(Unicode(150), nullable=False)
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
    def __init__(self, storage_path, blocks):
        self.storage_path = storage_path
        self.blocks = blocks
        self.open_block = None
        self.block_position = 0

    def read(self, size=-1):
        if size == 0:
            return b''

        elif self.open_block is None:
            try:
                path = self.blocks[self.block_position]
            except IndexError:
                return b''
            self.open_block = get_open_file(join_path(self.storage_path, path))

        binary = self.open_block.read(size)
        if size > 0:
            size -= len(binary)
            if size <= 0:
                return binary

        self.open_block = None
        self.block_position += 1
        binary += self.read(size)
        return binary
