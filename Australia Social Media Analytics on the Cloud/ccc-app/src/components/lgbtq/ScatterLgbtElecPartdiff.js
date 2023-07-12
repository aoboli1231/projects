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
export default function ScatterLgbtElectPartdiff({ data }) {
  const [laborData, setLaborData] = useState(null)
  const [libData, setLibData] = useState(null)

  function formatData(suburbData) {
    var labor = []
    var lib = []

    suburbData['features'].forEach(suburb => {
      if ((suburb['properties']['SAL_NAME21'] != undefined) && (suburb['properties']['lgbt_pos%'] != undefined) && 
        (suburb['properties']['male_female_part_diff'] != undefined) && (suburb['properties']['lgbt_total'] >= 10)) {
        
        var name = suburb['properties']['SAL_NAME21'];
        var pos_perc = round(suburb['properties']['lgbt_pos%']*100, 0);
        var m_f_diff = round(suburb['properties']['male_female_part_diff']*100, 0);
        if (suburb['properties']['labor%'] >= 0.5) {
          labor.push({'suburb': name, 'lgbt_pos%': pos_perc, 'male_female_part_diff': m_f_diff});
        } else if (suburb['properties']['labor%'] < 0.5) {
          lib.push({'suburb': name, 'lgbt_pos%': pos_perc, 'male_female_part_diff': m_f_diff});
        }
      }
    })

    setLaborData(labor)
    setLibData(lib)
  }

  useEffect(() => {
    formatData(data)
  }, [])
  

  return (
    <>
    <ChartTitle title="Male-Female Emp % diff vs Postive Sentiment % (Suburbs)" />
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
      <XAxis type="number" dataKey="male_female_part_diff" name="Male-Female Emp %"
        label={{ value: 'Male-Female Emp %', angle: 0, position: 'insideBottomRight', offset: -10 }}/>
      <YAxis type="number" dataKey="lgbt_pos%" name="Pos. Sent. %"
        label={{ value: 'Pos. Sent. %', angle: -90, position: 'insideLeft' }}
        />
      <Tooltip cursor={{ strokeDasharray: '3 3' }} />
      <Scatter name="Liberal National" data={libData} fill="#4575b4" label />
      <Scatter name="Labor" data={laborData} fill="#d73027" label />
      <Legend align='center' verticalAlign='bottom' />
      <Label />
    </ScatterChart>
    </>
  )
}
