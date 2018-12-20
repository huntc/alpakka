/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import akka.stream.Materializer
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{BidiFlow, Flow, Source}
import akka.util.ByteString

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Promise

object Mqtt {

  /**
   * Create a bidirectional flow that maintains client session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param session the MQTT client session to use
   * @return the bidirectional flow
   */
  def clientSessionFlow[A](
      session: MqttClientSession
  )(
      implicit mat: Materializer
  ): BidiFlow[Command[A], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[A]], NotUsed] = {
    import mat.executionContext
    val commandFlowCompleted = Promise[Done]
    BidiFlow.fromFlows(
      session.commandFlow.watchTermination() {
        case (e, done) =>
          done.foreach(commandFlowCompleted.success)
          e
      },
      session.eventFlow.merge(
        Source
          .fromFuture(commandFlowCompleted.future)
          .flatMapConcat(_ => Source.empty),
        eagerComplete = true
      )
    )
  }

  /**
   * Create a bidirectional flow that maintains server session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param session the MQTT server session to use
   * @param connectionId a identifier to distinguish the client connection so that the session
   *                     can route the incoming requests
   * @return the bidirectional flow
   */
  def serverSessionFlow[A](
      session: MqttServerSession,
      connectionId: ByteString
  ): BidiFlow[Command[A], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[A]], NotUsed] =
    BidiFlow.fromFlows(session.commandFlow(connectionId), session.eventFlow(connectionId))
}
