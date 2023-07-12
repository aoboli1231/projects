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
import { Area, AreaChart, CartesianGrid, Legend, Tooltip, XAxis, YAxis } from "recharts";


export default function AreaStateMonth({ data }) {

    var monthData = []

    Object.keys(data).forEach(state => {
        Object.keys(data[state]).forEach(month => {
            if (monthData[month] == undefined) {monthData[month] = {}}
            monthData[month][state] = data[state][month]['pos%'] - data[state][month]['neg%']
        })
    })

    monthData = Object.keys(monthData).map(month => {
        monthData[month].month = month
        return(monthData[month])
    })

    monthData.sort((a, b) => a.month.localeCompare(b.month))

    const colors = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#ffff33','#a65628','#f781bf']

    console.log(monthData)

    return(
        <AreaChart width={730} height={250} data={monthData}
            margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <defs>
            {
              Object.keys(data).map((state, index) => (
                <linearGradient id={state} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={colors[index]} stopOpacity={0.8}/>
                <stop offset="95%" stopColor={colors[index]} stopOpacity={0}/>
                </linearGradient>
              ))
            }
            </defs>
            <XAxis dataKey="name" />
            <YAxis />
            <CartesianGrid strokeDasharray="3 3" />
            <Tooltip />
            {
              Object.keys(data).map((state, index) => (
                <Area type="monotone" dataKey={state} stroke={colors[index]} fillOpacity={1} fill={`url(#${state})`} />
              ))
            }
        </AreaChart>
    )
}