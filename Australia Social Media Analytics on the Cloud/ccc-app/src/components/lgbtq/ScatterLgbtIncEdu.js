/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { PureComponent, useEffect, useState } from 'react';
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Label,
} from 'recharts';
import { round } from '../../functions/round';
import ChartTitle from '../ChartTitle';
{/*
const data = [
  { x: 100, y: 200, z: 200 },
  { x: 120, y: 100, z: 260 },
  { x: 170, y: 300, z: 400 },
  { x: 140, y: 250, z: 280 },
  { x: 150, y: 400, z: 500 },
  { x: 110, y: 280, z: 200 },
];
*/}
export default function ScatterLgbtIncEdu({ data }) {
  const [highUniData, setHighUniData] = useState(null)
  const [lowUniData, setLowUniData] = useState(null)

  function formatData(suburbData) {
    var highUni = []
    var lowUni = []

    suburbData['features'].forEach(suburb => {
      if ((suburb['properties']['SAL_NAME21'] != undefined) && (suburb['properties']['lgbt_pos%'] != undefined) && 
        (suburb['properties']['median_income'] != undefined) && (suburb['properties']['lgbt_total'] >= 10)) {
        
        var name = suburb['properties']['SAL_NAME21'];
        var pos_perc = round(suburb['properties']['lgbt_pos%']*100, 0);
        var median_income = round(suburb['properties']['median_income'], 0);
        if (suburb['properties']['uni'] >= 0.25) {
          highUni.push({'suburb': name, 'lgbt_pos%': pos_perc, 'median_income': median_income});
        } else if (suburb['properties']['uni'] < 0.25) {
          lowUni.push({'suburb': name, 'lgbt_pos%': pos_perc, 'median_income': median_income});
        }
      }
        //console.log(subInfo)
    })

    setHighUniData(highUni)
    setLowUniData(lowUni)
  }

  useEffect(() => {
    formatData(data)
  }, [])
  

  return (
    <>
    <ChartTitle title="Median Income vs Postive Sentiment % (Suburbs)" />
    <ScatterChart
      margin={{
        top: 20,
        right: 20,
        bottom: 20,
        left: 20,
      }}
      width={700}
      height={400}
    >
      <CartesianGrid />
      <XAxis type="number" dataKey="median_income" name="Median Income ($)"
        label={{ value: 'Median Income ($)', angle: 0, position: 'insideBottomRight', offset: -10 }}/>
      <YAxis type="number" dataKey="lgbt_pos%" name="Pos. Sent. %"
        label={{ value: 'Pos. Sent. %', angle: -90, position: 'insideLeft' }}
        />
      <Tooltip cursor={{ strokeDasharray: '3 3' }} />
      <Scatter name="Degree % < 25" data={lowUniData} fill="#DD00DD" label />
      <Scatter name="Degree % > 25" data={highUniData} fill="#00BBBB" label />
      <Legend align='center' verticalAlign='bottom' />
      <Label />
    </ScatterChart>
    </>
  )
}
