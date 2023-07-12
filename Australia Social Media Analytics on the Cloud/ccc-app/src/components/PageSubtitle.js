/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import { Typography } from "@mui/material";
import React from "react";


export default function PageSubtitle({ subtitle }) {
    return (
        <Typography variant="h4" align="center" sx={{ marginBottom: "0.5em" }}>{subtitle}</Typography>
    )
}