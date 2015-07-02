0.1 (unreleased)
================

Features
--------

- Add unit tests

- Add documentation

- Add ines translations

- api.core: revert actions

- load api modules with venusian

- script to create babel templates / update / compile

- script to read ini run application and compile apidocjs


0.1a3 (unreleased)
==================

Features
--------

- New api module, ``api.core.BaseCoreIndexedSession``. This module is equals to ``api.core.BaseCoreSession`` but indexs all information with ``Whoosh`` package.

- ``csv`` renderer now accepts ``request.params``: `CSVDelimiter`, `CSVQuoteChar` and `CSVLineTerminator`

- Added method ``api.jobs.BaseJobsSession.get_active_jobs``. This gives all registered jobs and stats

- Added method ``api.jobs.BaseJobsSession.immediate_job_run`` to call and run a job

- Added PT translations

- ``cleaner.clean_phone_number`` to clean telephone / mobile numbers

- ``utils.validate_skype_username`` to validate skype usernames

- ``utils.validate_phone_number`` to validate telephone / mobile numbers

- Create dir folders for ``api.mailer`` (required by repoze.sendmail)

- ``api.utils.requests_limit_decorator`` limit requests, by ip address, to a api method

- ``url.open_url`` opens a URL

- ``url.get_url_body`` opens an URL and read the body as unicode

- ``url.get_url_file`` opens a URL and read the body

- ``url.open_json_url`` opens a URL and decode JSON body

- ``url.ping_url`` check is URL is valid

- ``validator.validate_code`` validate some code using JSON keys to read colander validators or ``ines.validator.*``

- ``utils.is_prime`` to check if number is a prime

- ``utils.find_next_prime`` return next prime number

- ``utils.PaginationClass`` to use as pagination class in pagination views

- Added method ``asdict`` to ``request.ApplicationsConnector``. Available as ``request.applications.asdict()``

- ``ines.request.DELETE`` reads data from body (POST) or QUERY_STRING (GET)


Bug Fixes
---------

- Choose correct SessionManager for api modules


0.1a2 (2015-06-20)
==================

Features
--------

- ``utils.make_unique_hash`` now receives a ``length`` attribute

- Changed request cache located at ``request.cache`` to ``request.session_cache``

- Changed filesystem cache located at ``request.fcache`` to ``request.cache``

- New function ``utils.remove_file`` to remove system files, ignoring some errors (with retry loop)

- New function ``utils.move_file`` to move a file ignoring some system errors

- New function ``convert.make_sha256_no_cache`` to ignore ``convert.make_sha256`` cache system

- New function ``utils.get_file_binary`` to get a file binary ignoring some system errors

- New function ``utils.put_binary_on_file`` to write/append a file ignoring some system errors

- New function ``utils.put_binary_on_file`` to write/append a file ignoring some system errors

- New function ``system.thread_is_running`` to check running threads, registered with ``system.register_thread`` or started with ``system.start_system_thread``

- New function ``convert.convert_timezone`` update datetime value to the selected timezone

- New function ``convert.maybe_string`` to convert value to string if exists, else return ``None``

- New function ``utils.path_unique_code`` create file path unique code using sha256

- New function ``utils.file_unique_code`` create file unique code using sha256

- New function ``utils.string_unique_code`` create string unique code using sha256

- New function ``mimetype.find_mimetype`` to find mimemtype using filename and/or file binary

- New sql filter function ``api.database.sql.date_in_period_filter`` to create a SQLAlchemy filter to find items in a period of time

- New cache type ``cache.SaveMeMemcached`` uses Memcached system

- New locks type ``cache.LockMeMemcached`` uses Memcached system

- Improve ``convert.make_sha256`` speed using ``repoze.lru.LRUCache``

- Improve ``cache.CacheMe.get_file_path`` speed using ``repoze.lru.LRUCache``

- Improve ``locks.LockMe.get_file_path`` speed using ``repoze.lru.LRUCache``

- Improve ``convert.camelcase`` speed using ``repoze.lru.LRUCache``

- Improve ``convert.uncamelcase`` speed using ``repoze.lru.LRUCache``

- New properties added to ``authentication.AuthenticatedSession``, ``is_user`` and ``is_apikey``

- Added ORM query to ines.api.core

- New module ``api.policy.BaseTokenPolicySession``. Helper for authentication using token

- New module ``api.mailer``. Helper for smtp mailer.

- New method ``api.route_url``. Creates the application route_url like request.route_url


Bug Fixes
---------

- Deal with ``IOError.errno`` and ``OSError.errno`` don't ignore them


Backwards Incompatibilities
---------------------------

- ines now requires SQLAlchemy >= 1.0.0

- ``api_cache_decorator`` now receives argument ``expire_cache`` to delete saved info, instead of ``expire``

- ``locks.LockTimeout`` moved to ``exceptions.LockTimeout``

- ``locks.remove_file_quietly`` moved to ``utils.remove_file_quietly``

- ``cache.make_dir`` moved to ``utils.make_dir``

- ``locks.LockMe`` now receives argument ``path`` instead of ``lock_path``

- ``locks.LockMe.clean_junk_locks`` don't receives any argument. Use ``locks.LockMe.clean_junk_locks_as_daemon`` instead.

- ``api.database.BaseSQLSessionManager`` moved to ``api.database.sql.BaseSQLSessionManager``

- ``api.database.BaseSQLSession`` moved to ``api.database.sql.BaseSQLSession``

- ``api.database.sql.BaseDatabaseSessionManager`` moved to ``api.database.BaseDatabaseSessionManager``

- ``api.database.sql.BaseDatabaseSession`` moved to ``api.database.BaseDatabaseSession``

- ines no longer depends on ``repoze.tm2``. If you use ``ines.middleware.repozerm.RepozeTMMiddleware`` or ``ines.api.database.sql.BaseSQLSessionManager`` or ``ines.api.core.BaseCoreSessionManager`` or ``ines.api.database.sql.BaseSQLSession`` or ``ines.api.core.BaseCoreSession`` you need to define ``repoze.tm2`` in setup requirements.

- ines no longer depends on ``transaction``. If you use ``ines.api.database.sql.BaseDatabaseSessionManager`` or ``ines.api.core.BaseCoreSessionManager`` or ``ines.api.database.sql.BaseSQLSession`` or ``ines.api.core.BaseCoreSession`` you need to define ``transaction`` in setup requirements.

- ines no longer depends on ``zope.sqlalchemy``. If want to keep using this as sqlalchemy session extension, you need to define it on config settings like `sql.session_extension = zope.sqlalchemy:ZopeTransactionExtension` and define ``zope.sqlalchemy`` in setup requirements.

- ines.api.*.settings now reads global settings keys starting with ines.api.*.__api_name__


Dependencies
------------

- ines now depends on ``repoze.lru``

- ines now depends on ``venusian``


0.1a1 (2015-05-06)
==================

- Initial release.
