/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { useEffect, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, Tooltip, XAxis, YAxis } from "recharts";
import ChartTitle from "../ChartTitle";


export default function LinesHourly({ data, mastData }) {

  var hourData = []

  Object.keys(data).forEach(state => {
    Object.keys(data[state]).forEach(hour => {
      if (hourData[hour] == undefined) {hourData[hour] = {}}
      hourData[hour][state] = data[state][hour]
      var mastKey = (hour[0]=="0") ? hour[1] : hour
      hourData[hour]["mastodon"] = mastData[mastKey]
    })
  })

  hourData = Object.keys(hourData).map(hour => {
    hourData[hour].hour = hour
    return(hourData[hour])
  })

  hourData.sort((a, b) => a.hour.localeCompare(b.hour))

  const colors = ['#a6cee3','#1f78b4','#b2df8a','#33a02c','#fb9a99','#e31a1c','#fdbf6f','#ff7f00','#6734eb']

  return(
    <>
      <ChartTitle title="Number of tweets (per state) and toots by time of day" />
      <LineChart width={700} height={300} data={hourData}
          margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="hour" name="Hours" label={{ value: 'Time (Hours)', angle: 0, position: 'insideBottomRight', offset: -5 }}/>
          <YAxis />
          <ReferenceLine y={0} stroke="black" />
          <Tooltip />
          <Legend align='left' verticalAlign='bottom' />
          {
            Object.keys(data).map((state, index) => (
              <Line type="natural" dataKey={state} stroke={colors[index]} />
            ))
          }
          <Line type="natural" dataKey="mastodon" stroke={colors[colors.length-1]} />
      </LineChart>
    </>
    )
}