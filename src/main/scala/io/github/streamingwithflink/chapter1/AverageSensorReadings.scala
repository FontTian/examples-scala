/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter1

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object AverageSensorReadings {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the local streaming execution environment
    // 设置本地环境（本地集群）
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Set up remote stream execution environment
    // 设置远程集群
    // val env = StreamExecutionEnvironment.createRemoteEnvironment("host",9703,"path/to/jarFile.jar")

    // use event time for the application
    // 设置应用时间为事件时间，另外两种是处理时间（ProcessingTime）和摄取时间（IngestionTime）
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    // 在流式数据源中获取DataStream[SensorReading]对象
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      // 利用SensorSource SourceFunction获取传感器读数
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      // 分配时间戳和水位线（事件时间所需）
      // .assignTimestampsAndWatermarks(new SensorTimeAssigner)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius using an inlined map function
      // 使用内联的lambda函数将华氏温度转换为摄氏度
      .map( r =>
      SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      // organize stream by sensorId
      // 按照传感器id组织数据
      .keyBy(_.id)
      // group readings in 1 second windows
      // 将读数按照一秒的滚动窗口分组
      .timeWindow(Time.seconds(1))
      // compute average temperature using a user-defined function
      // 使用用户自定义函数计算平均温度
      .apply(new TemperatureAverager)

    // print result stream to standard out
    // 将结果流打印到标准输出
    avgTemp.print()

    // execute application
    // 开始执行应用
    env.execute("Compute average sensor temperature")
  }
}

/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
    sensorId: String,
    window: TimeWindow,
    vals: Iterable[SensorReading],
    out: Collector[SensorReading]): Unit = {

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
