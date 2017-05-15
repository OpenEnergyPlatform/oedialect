from sqlalchemy.engine import Connection
import sqlalchemy
import psycopg2

class OEConnection():
    """

    """

    """
        Connection methods
    """

    def __init__(self):
        pass
    """
        TODO: Look at PGDialect in sqlalchemy.dialects.postgresql.base
    """
    def close(self, *args, **kwargs):
        raise NotImplementedError

    def commit(self, *args, **kwargs):
        raise NotImplementedError

    def rollback(self, *args, **kwargs):
        raise NotImplementedError

    def cursor(self, *args, **kwargs):
        cursor = OECursor()
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

    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k], params)
            return jsn
        elif type(jsn) == list:
            return list(map(lambda x: self.__replace_params(x, params), jsn))
        elif type(jsn) in [str, sqlalchemy.sql.elements.quoted_name, sqlalchemy.sql.elements._truncated_label]:
            print(jsn, params)
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

    def execute(self, context, params):
        def get_column(dic):
            col = sqlalchemy.Column()

            dic['nullable'] = dic['null_ok']
            col.__dict__ = dic
            col.table = None
            return col

        query = self.__replace_params(context.compiled.string, params)
        # query = context.compiled.string
        # print query
        t = query.pop('type')
        r = post(t, query)

        result = r['content']

        if 'description' in result:
            self.description = result['description']
            print("description", self.description)
            self.data = result['data']
            self.rowcount = len(self.data)
    def close(self):
        print('Close cursor')


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