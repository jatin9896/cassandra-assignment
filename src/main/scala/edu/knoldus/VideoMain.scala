package edu.knoldus
import com.datastax.driver.core.Cluster
/**
  * Created by knoldus on 9/4/17.
  */
object VideoMain {
  def main(args: Array[String]) {
    val videoHandler=new VideoHandler
    //queryClass.userByEmail("my@gmail.com")
    val userHandler=new UserHandler
    userHandler.insertIntoUser(User("hello@gmail.com","hello",2))
    videoHandler.insertIntoVideo(Video(2,"khaab",3,2017))
    videoHandler.videoByName("khaab")
    videoHandler.videoByUseridYear(2,2015)
    videoHandler.videoByUseridYearDesc(1,2015)
  }

}
