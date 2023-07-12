/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import './App.css';
import React, { useEffect, useState } from 'react';
import { Routes, Route, Router } from 'react-router-dom';
import {
  CssBaseline, Toolbar, Box, createTheme, Button, ThemeProvider
} from "@mui/material";
import Scenario1 from './pages/Scenario1';
import Scenario2 from './pages/Scenario2';
import Scenario3 from './pages/Scenario3';
import NotFound from './components/NotFound';
import Home from './pages/Home';
import SideNav from './components/SideNav';
import ScrollToTop from './components/ScrollToTop';
import { getData } from './functions/fetchData';

function App() {

  const theme = createTheme({
    palette: {
      background: {
        default: "#d4dadc"
      }
    }
  });

  const [stateData, setStateData] = useState(null)
  const [suburbData, setSuburbData] = useState(null)
  const [title, setTitle] = useState('CCC Team 14')
  useEffect(() => {
    getData('/state_geojson', setStateData)
    getData('/suburb_geojson', setSuburbData)
  }, [])

  const updateTitle = (newTitle) => {
    setTitle(newTitle)
  }
  return (
    <><ThemeProvider theme={theme}>
      <CssBaseline />
      <ScrollToTop />
      <Box sx={{ display: 'flex' }}>
        <CssBaseline />
        <SideNav title={title} />
        <Box
          component="main"
          sx={{ flexGrow: 1, bgcolor: 'background.default', p: 3 }}
        >
          <Toolbar />
          <Routes>
            <Route path='/' element={<Home updateTitle={updateTitle} title="Home" />} />
            <Route path='/scenario1' element={<Scenario1 stateData={stateData} suburbData={suburbData} title="Scenario1: Ukraine Russia War" updateTitle={updateTitle} />} />
            <Route path='/scenario2' element={<Scenario2 stateData={stateData} suburbData={suburbData} title="Scenario2: LGBTQ" updateTitle={updateTitle} />} />
            <Route path='/scenario3' element={<Scenario3 title="Scenario3: Time" updateTitle={updateTitle} />} />
            <Route path='*' element={<NotFound />} />
          </Routes>
        </Box>
      </Box>
    </ThemeProvider>

    </>
  );
}

export default App;
