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
import { NavLink } from "react-router-dom";
import {
    AppBar,
    Toolbar,
    Typography
  } from "@mui/material";


export default function Nav() {
    return (
        <>
        <AppBar position="relative">
            <Toolbar>
                <Typography variant="h4">
                Cluster and Cloud Computing - A2
                </Typography>
                <div>
                    <NavLink to='/'>Home</NavLink>
                    <NavLink to='/scenario1'>Scenario 1</NavLink>
                    <NavLink to='/scenario2'>Scenario 2</NavLink>
                    <NavLink to='/scenario3'>Scenario 3</NavLink>
                </div>
            </Toolbar>
        </AppBar>
{/*
        <AppBar position='relative'>
            <nav>
            <ul>
                <li><NavLink to='/Scenario1'>Scenario 1</NavLink></li>
            </ul>
            <ul>
                <li><NavLink to='/Scenario2'>Scenario 2</NavLink></li>
            </ul>
            <ul>
                <li><NavLink to='/Scenario3'>Scenario 3</NavLink></li>
            </ul>
            </nav>
        </AppBar>
    */}
        </>
    )
}