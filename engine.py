from sqlalchemy.engine import Connection, Transaction
import sqlalchemy
import psycopg2
import login
import requests
import json

class OEConnection():
    """

    """

    """
        Connection methods
    """

    def __init__(self):
        response = post('open_raw_connection', {})['content']
        self.__id = response['connection_id']
        self.__transactions = set()
        self.__cursors = set()
        self.__closed = False
    """
        TODO: Look at PGDialect in sqlalchemy.dialects.postgresql.base
    """
    def close(self, *args, **kwargs):
        pass
        #response = post('close_raw_connection', {})

    def commit(self, *args, **kwargs):
        for key in self.__transactions:
            self.__transactions[key].commit()

    def rollback(self, *args, **kwargs):
        for key in self.__transactions:
            self.__transactions[key].rollback()

    def cursor(self, *args, **kwargs):
        cursor = OECursor(self.__id)
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


class OECursor:
    description = None

    def __init__(self, connection_id):
        response = post('open_cursor', {'connection_id': connection_id})['content']
        self.__id = response['cursor_id']

    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k], params)
            return jsn
        elif type(jsn) == list:
            return list(map(lambda x: self.__replace_params(x, params), jsn))
        elif type(jsn) in [str, sqlalchemy.sql.elements.quoted_name, sqlalchemy.sql.elements._truncated_label]:
            return (jsn % params).strip("'<>").replace('\'', '\"')
            # print "UNKNOWN TYPE: %s @ %s " % (type(jsn),jsn)
        else:
            raise Exception("Unknown jsn type (%s) in %s"%(type(jsn),jsn))

    def fetchone(self):
        return self.data.pop() if self.data else None

    def fetchall(self):
        return self.fetchmany(self.rowcount)

    def fetchmany(self, size):
        if not self.data:
            return self.data
        resu = self.data[:size]
        self.data = self.data[size:]
        return resu

    def execute(self, query, params=None):
        if not isinstance(query, dict):
            query = query.string
        if params:
            query = self.__replace_params(query, params)
        # query = context.compiled.string
        # print query
        command = query.pop('type')
        return self.__execute_by_post(command, query)

    def close(self):
        post('close_cursor', {'cursor_id': self.__id})


    def __execute_by_post(self, command, query):

        r = post(command,query)

        result = r['content']
        if 'description' in result:
            self.description = result['description']
            self.data = result['data']
            self.rowcount = len(self.data)
        else: # Test
            self.data = [result]
            self.rowcount = 1

urlheaders = {
    'Content-type': 'application/x-www-form-urlencoded',
    'Accept': 'text/javascript, text/html, application/xml, text/xml, application/json */*',
    'Accept-Encoding': 'gzip,deflate,sdch',
    'Accept-Charset': 'utf-8',
    'Authorization': login.secret_key
}

class ConnectionException(Exception):
    pass

def post(suffix, query):
    query = json.dumps(query)
    ans = requests.post(
        'http://localhost:8000/api/%s' % suffix,
        data={'query':query}, headers=urlheaders)

    if ans.status_code == 500:
        raise ConnectionException(ans)

    # ans = requests.post('http://193.175.187.164/data/api/action/dataconnection_%s' % suffix, data=query, headers=urlheaders)
    # if ans.status_code == 400:
    return ans.json()

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