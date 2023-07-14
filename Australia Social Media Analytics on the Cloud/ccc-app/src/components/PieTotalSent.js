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
import { PieChart, Pie } from "recharts";


export default function PieTotalSent() {
    const [RUwar, setRUwar] = useState(null)
    
    const getData = () => {
        console.log("Fetching Data Test")
        fetch('http://45.113.234.176:5000/sentiment/RUwar', {
            mode: 'cors',
            header:{
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        })
        .then((res) => res.json())
        .then((data) => Object.keys(data).map(key => ({"sentiment_type": key, "count": data[key]})))
        .then((data) => {setRUwar(data)})
        .catch(err => {
            console.log('======failed to fetch data=======');
            console.log(err);
        })
    }

    useEffect(() => {
        getData()
    }, [])
    
    return (
        <>
        <PieChart width={"0.75em"} height={"auto"}>
            <Pie data={RUwar} dataKey="count" nameKey="sentiment_type" cx="50%" cy="50%" outerRadius={50} fill="#8884d8" label
            />
        </PieChart>
        </>
    )
}