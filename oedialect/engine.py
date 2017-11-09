import json

import requests
import sqlalchemy

from oedialect import login
from oedialect import error

from shapely import wkb

class OEConnection():
    """

    """

    """
        Connection methods
    """

    def __init__(self, host='localhost', port=80, user='', database='', password=''):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__token = password
        response = self.post('advanced/connection/open', {})['content']
        self._id = response['connection_id']
        self.__transactions = set()
        self.__cursors = set()
        self.__closed = False

    """
        TODO: Look at PGDialect in sqlalchemy.dialects.postgresql.base
    """

    def close(self, *args, **kwargs):
        response = self.post('advanced/connection/close', {'connection_id': self._id})


    def commit(self, *args, **kwargs):
        response = self.post('advanced/connection/commit', {'connection_id': self._id})

    def rollback(self, *args, **kwargs):
        for key in self.__transactions:
            self.__transactions[key].rollback()

    def cursor(self, *args, **kwargs):
        cursor = OECursor(self)
        self.__cursors.add(cursor)
        return cursor

    """
        Two-phase commit support methods
    """

    def xid(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_begin(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_commit(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_prepare(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_recover(self, *args, **kwargs):
        raise NotImplementedError

    def tpc_rollback(self, *args, **kwargs):
        raise NotImplementedError

    """
        DB API extension
    """

    def cancel(self, *args, **kwargs):
        raise NotImplementedError

    def reset(self, *args, **kwargs):
        raise NotImplementedError

    def set_session(self, *args, **kwargs):
        raise NotImplementedError

    def set_client_encoding(self, *args, **kwargs):
        raise NotImplementedError

    def set_isolation_level(self, *args, **kwargs):
        raise NotImplementedError

    def get_backend_pid(self, *args, **kwargs):
        raise NotImplementedError

    def get_dsn_parameters(self, *args, **kwargs):
        raise NotImplementedError

    def get_parameter_status(self, *args, **kwargs):
        raise NotImplementedError

    def get_transaction_status(self, *args, **kwargs):
        raise NotImplementedError

    def lobject(self, *args, **kwargs):
        raise NotImplementedError

    """
        Methods related to asynchronous support
    """

    def poll(self, *args, **kwargs):
        raise NotImplementedError

    def fileno(self, *args, **kwargs):
        raise NotImplementedError

    def isexecuting(self, *args, **kwargs):
        raise NotImplementedError


    def post(self, suffix, query, cursor_id=None):
        sender = requests.post
        if isinstance(query, dict) and 'request_type' in query:
            if query['request_type'] == 'put':
                sender = requests.put

        data = {'query': query}

        if cursor_id:
            data['cursor_id'] = cursor_id

        header = dict(urlheaders)
        header['Authorization'] = 'Token %s'%self.__token

        ans = sender(
            'http://{host}:{port}/api/v0/{suffix}'.format(host=self.__host, port=self.__port, suffix=suffix),
            json=data, headers=header)

        try:
            json_response = ans.json()
        except:
            raise ConnectionException('Answer contains no JSON: ' + repr(ans))

        if 400 <= ans.status_code < 600:
            raise ConnectionException(json_response['reason'] if 'reason' in json_response else 'No reason returned')

        return json_response

class OECursor:
    description = None

    def __init__(self, connection):
        self.__connection = connection
        try:
            response = self.__connection.post('advanced/cursor/open', {'connection_id': connection._id})
            if 'content' not in response:
                raise error.CursorError('Could not open cursor: ' + str(response['reason']) if 'reason' in response else 'No reason returned')
            response = response['content']
        except:
            raise
        self.__id = response['cursor_id']


    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k], params)
            return jsn
        elif type(jsn) == list:
            return list(map(lambda x: self.__replace_params(x, params), jsn))
        elif type(jsn) in [str, sqlalchemy.sql.elements.quoted_name,
                           sqlalchemy.sql.elements._truncated_label]:
            return (jsn % params).strip("'<>").replace('\'', '\"')
            # print "UNKNOWN TYPE: %s @ %s " % (type(jsn),jsn)
        elif isinstance(jsn, int):
            return jsn
        elif callable(jsn):
            return jsn(params)
        else:
            raise Exception("Unknown jsn type (%s) in %s" % (type(jsn), jsn))


    def fetchone(self):
        response = self.__connection.post('advanced/cursor/fetch_one', {}, cursor_id=self.__id)[
            'content']
        if response:
            for i, x in enumerate(self.description):
                # Translate WKB-hex to binary representation
                if x[1] == 17:
                    response[i] = wkb.dumps(wkb.loads(response[i], hex=True))
        return response

    def fetchall(self):
        data = self.__connection.post('advanced/cursor/fetch_all', {}, cursor_id=self.__id)[
            'content']
        return data

    def fetchmany(self, size):
        response = self.__connection.post('advanced/cursor/fetch_many', {'size': size}, cursor_id=self.__id)[
            'content']
        return response

    def execute(self, query, params=None):
        if not isinstance(query, dict):
            query = query.string
        query['cursor_id'] = self.__id
        if params:
            query = self.__replace_params(query, params)
        # query = context.compiled.string
        # print query
        command = query.pop('command')
        return self.__execute_by_post(command, query)

    def close(self):
        self.__connection.post('advanced/cursor/close', {}, cursor_id=self.__id)

    def __execute_by_post(self, command, query):

        response = self.__connection.post(command, query, cursor_id=self.__id)

        if 'content' in response:
            result = response['content']
            if result:
                if isinstance(result, dict):
                    if 'description' in result:
                        self.description = result['description']
                else:
                    return result
            else:
                return result



urlheaders = {
}


class ConnectionException(Exception):
    pass


"""
class OEExecutionContext_psycopg2(PGExecutionContext):
    @classmethod
    def _init_ddl(cls, dialect, connection, dbapi_connection, compiled_ddl):
        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect
        self.compiled = compiled = compiled_ddl
        self.isddl = True

        self.execution_options = compiled.statement._execution_options
        if connection._execution_options:
            self.execution_options = dict(self.execution_options)
            self.execution_options.update(connection._execution_options)

        if not dialect.supports_unicode_statements:
            self.unicode_statement = ""  # util.text_type(compiled)
            self.statement = ""  # dialect._encoder(self.unicode_statement)[0]
        else:
            self.statement = ""  # self.unicode_statement = util.text_type(compiled)

        self.cursor = self.create_cursor()

        self.compiled_parameters = []

        if dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]

        return self


    def get_result_proxy(self):
        # TODO: ouch
        if logger.isEnabledFor(logging.INFO):
            self._log_notices(self.cursor)

        if self.__is_server_side:
            return _result.BufferedRowResultProxy(self)
        else:
            return _result.ResultProxy(self)


    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()
        cursor = OECursor(self)
        self.__is_server_side = False
        return cursor
"""
