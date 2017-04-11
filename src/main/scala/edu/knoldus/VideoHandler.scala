package edu.knoldus

import java.util.logging.Logger

import com.datastax.driver.core.Cluster
import org.slf4j.LoggerFactory

class VideoHandler {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()

  val session = cluster.connect("KIP")
  val logger=LoggerFactory.getLogger(classOf[VideoHandler])
  def userByEmail(email:String) = {

    val res = session.execute(s"SELECT * FROM userbyemail where email='$email'")

    val iterate = res.iterator()

    while (iterate.hasNext) {

      logger.info("resultset : "+iterate.next())

    }

  }

  def videoByName(name:String) = {

    val res = session.execute(s"SELECT * FROM videobyname where video_name='$name'")

    val iterate = res.iterator()

    while (iterate.hasNext) {

      logger.info("resultset : "+iterate.next())

    }
    println()
  }

  def videoByUseridYear(id:Int,year:Int) = {

    val res = session.execute(s"SELECT * FROM videobyyear3 where userid=$id AND year>$year")

    val iterate = res.iterator()

    while (iterate.hasNext) {

      logger.info("resultset : "+iterate.next())

    }
    println()
    //cluster.close()
  }

  def videoByUseridYearDesc(id:Int,year:Int) = {

    val res = session.execute(s"SELECT * FROM videobyyearonuser where userid=$id AND year>$year ORDER BY year DESC")

    val iterate = res.iterator()

    while (iterate.hasNext) {

      logger.info("resultset : "+iterate.next())

    }
    println()
    cluster.close()
  }

  def insertIntoVideo(video: Video): String = {
    val res1 = session.execute(s"insert into video(video_name , userid , year , video_id ) VALUES ( ${video.video_name},${video.userid},${video.year},${video.video_id});")
    logger.info("data result of insertion in video table" + res1)
    val res2=session.execute(s"insert into videobyname (video_name , video_id ) VALUES ( ${video.video_name},${video.video_id}) ;")
    logger.info("data result of insert into video by name" + res2)
    val res3=session.execute(s"INSERT INTO videobyyear3(userid , year , videoid ) values (${video.userid},${video.year},${video.video_id})")
    logger.info("data result of insert into video by year" + res3)
     val res4=session.execute(s"insert into videobyyearonuser (userid , year , video_id ) VALUES ( ${video.userid},${video.year},${video.video_id}); ")
    logger.info("data result of insert into video by year on user " + res4)


    res1.toString
  }

}
