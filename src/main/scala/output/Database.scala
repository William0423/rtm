
import java.sql.{Connection, DriverManager, ResultSet}

case class DBParams(
    subprotocol: String,
    host: String,
    port: Int,
    user: String,
    passwd: String,
    db: String
)

class DBConnection(conn: Connection, params: DBParams) {
    val statment = conn.createStatement()
    val createTime = System.currentTimeMillis()
    var buffer = new scala.collection.mutable.ListBuffer[String]

    def getParams: DBParams = params
    
    def execute(sql: String, values: Any*) = {
        val finalSql = sql.format(values)

        statment.execute(finalSql)
    }

    def execute(sql: String) = {
        if (sql.nonEmpty) {
            statment.execute(sql)
            buffer += sql
        }
    }

    def addBatch(sql: String) = {
        if (sql.nonEmpty) {
            statment.addBatch(sql)
            buffer += sql
        }
    }

    def isValid() = {
        conn.isValid(0)
    }

    def executeBatch() = {
        statment.executeBatch()
        conn.commit()
        buffer.clear()
    }

    def getBuffer() = buffer
}

class DBConnectionPool(params: DBParams) {

    val queue = new scala.collection.mutable.SynchronizedQueue[DBConnection]
    //  "jdbc:mysql://localhost:3306/DBNAME"
    val url = "jdbc:%s://%s:%d/%s?useServerPrepStmts=false&rewriteBatchedStatements=true".format(params.subprotocol, params.host, params.port, params.db)

    def createConnection: DBConnection = {
        //classOf[com.mysql.jdbc.Driver]
        val conn = DriverManager.getConnection(url, params.user, params.passwd)
        conn.setAutoCommit(false)
        new DBConnection(conn, params)
    }
    
    def borrow(): DBConnection = {
        try {
            val c = queue.dequeue()
            if (!c.isValid()) {
                createConnection
            } else {
                c
            }
        } catch {
            case _ : NoSuchElementException => createConnection
        }
    }

    def returnConnection(p: DBConnection) {
        queue.enqueue(p)
    }
}

object DBConnectionFactory {

    var pool = new scala.collection.mutable.HashMap[DBParams, DBConnectionPool]

    def getConnPool(params: DBParams) = synchronized {
        pool.getOrElseUpdate(params, {
            new DBConnectionPool(params)
        })
    }

    def getConnection(params: DBParams) = {
        getConnPool(params).borrow()
    }

    def returnConnection(conn: DBConnection) = {
        getConnPool(conn.getParams).returnConnection(conn)
    }
}
