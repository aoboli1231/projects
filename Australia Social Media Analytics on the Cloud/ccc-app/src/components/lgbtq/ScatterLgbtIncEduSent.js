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
export default function ScatterLgbtIncEduSent({ data }) {
  const [highUniData, setHighUniData] = useState(null)
  const [lowUniData, setLowUniData] = useState(null)

  function formatData(suburbData) {
    var highUni = []
    var lowUni = []

    let i = 0
    let j = 0

    suburbData['features'].forEach(suburb => {
      if ((suburb['properties']['SAL_NAME21'] != undefined) && (suburb['properties']['lgbt_pos%'] != undefined) && 
        (suburb['properties']['median_income'] != undefined) && (suburb['properties']['lgbt_total'] >= 5)) {
        
        var name = suburb['properties']['SAL_NAME21'];
        var pos_prop = round(suburb['properties']['lgbt_pos%'], 2);
        var median_income = round(suburb['properties']['median_income'], 0);
        if (suburb['properties']['uni'] >= 0.25) {
          highUni.push({'suburb': name, 'lgbt_pos%': pos_prop, 'median_income': median_income});
          i += 1;
        } else if (suburb['properties']['uni'] < 0.25) {
          lowUni.push({'suburb': name, 'lgbt_pos%': pos_prop, 'median_income': median_income});
          j += 1;
        }
      }
        //console.log(subInfo)
    })

    setHighUniData(highUni)
    setLowUniData(lowUni)

    console.log("i", i)
    console.log("j", i)
  }

  useEffect(() => {
    formatData(data)
  }, [])
  

  return (
    <>
    <ChartTitle title="Median Income vs Proportion of postive Sentiment (Suburbs)" />
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
      <XAxis type="number" dataKey="median_income" name="median income" unit="$" 
        label={{ value: 'Median Income', angle: 0, position: 'insideBottomRight', offset: -10 }}/>
      <YAxis type="number" dataKey="lgbt_pos%" name="positive proportion"
        label={{ value: 'pos. prop', angle: -90, position: 'insideLeft' }}
        />
      <Tooltip cursor={{ strokeDasharray: '3 3' }} />
      <Scatter name="Degree % < 0.25" data={lowUniData} fill="#DD00DD" label />
      <Scatter name="Degree % > 0.25" data={highUniData} fill="#00BBBB" label />
      <Legend align='center' verticalAlign='bottom' />
      <Label />
    </ScatterChart>
    </>
  )
}
