/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, Tooltip, XAxis, YAxis } from "recharts";
import ChartTitle from "../ChartTitle";
import { round } from "../../functions/round";


export default function LinesStateMonthMag({ data }) {

    var monthData = []

    Object.keys(data).forEach(state => {
        Object.keys(data[state]).forEach(month => {
            if (monthData[month] == undefined) {monthData[month] = {}}
            monthData[month][state] = round((data[state][month]['ave_mag']), 3)
        })
    })

    monthData = Object.keys(monthData).map(month => {
        monthData[month].month = month
        return(monthData[month])
    })

    monthData.sort((a, b) => a.month.localeCompare(b.month))

    const colors = ['#1b9e77','#d95f02','#7570b3','#e7298a','#66a61e','#e6ab02','#a6761d','#666666']

    return(
      <>
        <ChartTitle title="Magnitude of sentiment" />
        <LineChart width={700} height={300} data={monthData}
            margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" name="YYYY-MM" label={{ value: 'YYYY-MM', angle: 0, position: 'insideBottomRight', offset: -10 }}/>
            <YAxis />
            <ReferenceLine y={0} stroke="black" />
            <Tooltip />
            <Legend align='left' verticalAlign='bottom' />
            {
              Object.keys(data).map((state, index) => (
                <Line type="natural" dataKey={state} stroke={colors[index]} />
              ))
            }
        </LineChart>
      </>
    )
}