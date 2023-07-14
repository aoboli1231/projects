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
  ResponsiveContainer,
  Brush
} from "recharts";
import ChartTitle from "../ChartTitle";
import { Container, Typography } from "@mui/material";
import { round } from "../../functions/round";

export default function BarTwitterLangSent({ data }) {
  
    const [plotData, setPlotData] = useState(null)

    // Remove undefined, and assign ranks
    useEffect(() => {
      data.splice(1,1)
      Object.keys(data).forEach((key, index) => {
        data[key].rank = index+1
      })
      data.splice(10)
      setPlotData(data)
    }, [])

    return (
        <Container>
            <ChartTitle title="Sentiment Prop. (Twitter)" />
            <BarChart
                stackOffset="none"
                width={300}
                height={300}
                data={plotData}
                margin={{
                    top: 10,
                    right: 5,
                    left: 5,
                    bottom: 10
                }}
            >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="language" name="Language" label={{ value: 'Language', angle: 0, position: 'insideBottomRight', offset: -10 }} />
                <YAxis />
                <Tooltip />
                <Legend />
                <ReferenceLine y={0} stroke="#000" />
                <Brush dataKey="rank" height={30} stroke="#8884d8" />
                <Bar dataKey="pos%" fill="#00BBBB" stackId="stack" />
                <Bar dataKey="neu%" fill="grey" stackId="stack" />
                <Bar dataKey="neg%" fill="#DD00DD" stackId="stack" />
            </BarChart>
            <Typography variant="caption" align="center">Excludes English (en) and undefined (und)</Typography>
        </Container>
  );
}

