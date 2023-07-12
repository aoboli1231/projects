/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { useState, useEffect } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer
} from "recharts";
import ChartTitle from "./ChartTitle";

export default function BarLang() {
    const [langSentData, setLangSentData] = useState([])

    const getData = () => {
        fetch('http://localhost:3001/langSent', {
          header:{
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          }
        })
        .then((res) => res.json())
        .then((data) => {setLangSentData(data)})
        .catch(err => {
          console.log('======failed to fetch data=======');
          console.log(err);
      });
      }

    useEffect(() => {
        getData()
    }, [])


    return (
        <div>
            <ChartTitle title="Language Mean Sentiment" />
            <BarChart
                width={300}
                height={300}
                data={langSentData}
                margin={{
                    top: 25,
                    right: 20,
                    left: 20,
                    bottom: 10
                }}
            >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="lang" />
                <YAxis />
                <Tooltip />
                <Legend />
                <ReferenceLine y={0} stroke="#000" />
                <Bar dataKey="count" fill="#00BBBB" />
            </BarChart>
            
        </div>
  );
}

