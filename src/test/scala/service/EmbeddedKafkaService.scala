package service

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class EmbeddedKafkaService(port:Int=9092, zkport:Int=2182){
  private implicit var testKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(port, zkport)
  def start(): Unit ={
    EmbeddedKafka.start()
  }
  def stop(): Unit ={
    EmbeddedKafka.stop()
  }

  def publish[K,T](topic: String, key: K, message: T, keySerializer: Serializer[K], messageSerializer: Serializer[T]): Unit = {
    EmbeddedKafka.publishToKafka(topic, key,  message)(testKafkaConfig, keySerializer, messageSerializer)
  }

  def publish[K,T,V](topic: String, obj:V, getKeyFunc:V => K, getMessageFunc:V=>T, keySerializer: Serializer[K], messageSerializer: Serializer[T]): Unit = {
    EmbeddedKafka.publishToKafka(topic, getKeyFunc(obj),  getMessageFunc(obj))(testKafkaConfig, keySerializer, messageSerializer)
  }

  def publishSeq[K,T,V](topic: String, objs:Seq[V], getKeyFunc:V => K, getMessageFunc:V=>T, keySerializer: Serializer[K], messageSerializer: Serializer[T]): Unit = {
    objs.foreach(o=>EmbeddedKafka.publishToKafka(topic, getKeyFunc(o),  getMessageFunc(o))(testKafkaConfig, keySerializer, messageSerializer))
  }

  def consume[T](topic: String, number:Int, messageDeserializer: Deserializer[T]): List[T] = {
    EmbeddedKafka.consumeNumberMessagesFrom(topic, number, true)(testKafkaConfig, messageDeserializer)
  }
  def consume[T](topic: String, messageDeserializer: Deserializer[T]): T = {
    EmbeddedKafka.consumeFirstMessageFrom(topic, true)(testKafkaConfig, messageDeserializer)
  }

  def clear(): Unit ={
    stop()
    start()
  }
}