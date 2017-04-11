package edu.knoldus

import java.util.logging.Logger

import com.datastax.driver.core.Cluster
import org.slf4j.LoggerFactory

class UserHandler {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val logger=LoggerFactory.getLogger(classOf[UserHandler])
  val session = cluster.connect("KIP")
  def insertIntoUser(user:User): String ={

    val res1 = session.execute(s"insert into userbyemail (email , password, userid ) values (${user.email},${user.password},${user.userid});")
   logger.info("data result "+res1)
    res1.toString
  }
}
