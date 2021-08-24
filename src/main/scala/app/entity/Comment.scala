package app.entity

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.Timestamp

case class Comment(
                   id: Option[String],
                   name: Option[String],
                   comment: Option[String],
                   rating: Option[Int] = None,
                   ratingText: Option[String] = None,
                   timestamp: Option[Timestamp]
                 ){
  def serialise(): Array[Byte] = {
    Comment.serialise(this)
  }
}

object Comment{
  def deserialise(bytes: Array[Byte]): Comment = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject.asInstanceOf[Comment]
    ois.close()
    value
  }

  def serialise(comment: Comment): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(comment)
    oos.close()
    stream.toByteArray
  }
}
