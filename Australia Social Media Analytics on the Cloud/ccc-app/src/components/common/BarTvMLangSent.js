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
import { getData, getLangSentData } from "../../functions/fetchData";

export default function BarTvMLangSent({ twitterLangSent, mastLangSent }) {

    const [plotData, setPlotData] = useState(null)

    function combineData(twitterLangSent, mastLangSent) {
      var data = Object.keys(mastLangSent).map(key => {
        mastLangSent[key].language = key
        return(mastLangSent[key])
      })
      data.sort((a, b) => b.total-a.total)
      console.log(data)
      data = data.map(row => {
        const lang = row.language
        console.log(twitterLangSent[lang])
        row["twitter_pos%"] = twitterLangSent[lang]["pos%"]
        return[row]
      })
      return(data)
    }

    useEffect(() => {
      console.log("twitter", twitterLangSent)
      console.log("mast", mastLangSent)
      setPlotData(combineData(twitterLangSent, mastLangSent))
    }, [])

    return (
      <>
        {
          plotData &&
          <Container>
            <ChartTitle title="Day/Night Tweets (sort Total)" />
            <BarChart
                stackOffset="none"
                width={300}
                height={300}
                data={twitterLangSent}
                margin={{
                    top: 10,
                    right: 5,
                    left: 5,
                    bottom: 10
                }}
            >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="language" />
                <YAxis />
                <Tooltip />
                <Legend />
                <ReferenceLine y={0} stroke="#000" />
                <Brush dataKey="language" height={30} stroke="#8884d8" />
                <Bar dataKey="pos%" fill="#00BBBB" stackId="stack" />
                <Bar dataKey="neg%" fill="#DD00DD" stackId="stack" />
            </BarChart>
            <Typography variant="caption" align="center">Excludes English (en) and undefined (und)</Typography>
          </Container>
        }
    </>
  );
}

