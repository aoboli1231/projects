/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import * as React from 'react';
import { NavLink } from 'react-router-dom';
import Drawer from '@mui/material/Drawer';
import CssBaseline from '@mui/material/CssBaseline';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/joy/List';
import Typography from '@mui/material/Typography';
import Divider from '@mui/material/Divider';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import ListItemText from '@mui/joy/ListItemContent';
import GlobalStyles from '@mui/joy/GlobalStyles';
import Box from '@mui/joy/Box';
import Card from '@mui/joy/Card';

import IconButton from '@mui/joy/IconButton';
import Input from '@mui/joy/Input';
import Link from '@mui/joy/Link';
import LinearProgress from '@mui/joy/LinearProgress';
import Avatar from '@mui/joy/Avatar';
import ListItemContent from '@mui/joy/ListItemContent';
import ListItemDecorator from '@mui/joy/ListItemDecorator';
import HomeIcon from '@mui/icons-material/Home';
import Sheet from '@mui/joy/Sheet';
import SentimentVeryDissatisfiedIcon from '@mui/icons-material/SentimentVeryDissatisfied';
import {
    experimental_extendTheme as materialExtendTheme,
    Experimental_CssVarsProvider as MaterialCssVarsProvider,
    THEME_ID as MATERIAL_THEME_ID,
} from '@mui/material/styles';
import BedtimeIcon from '@mui/icons-material/Bedtime';
import TransgenderIcon from '@mui/icons-material/Transgender';
import { CssVarsProvider as JoyCssVarsProvider } from '@mui/joy/styles';
const drawerWidth = 240;

export default function SideNav({ title }) {
    const navInfo = [{ 'label': 'Home', 'link': '/', icon: <HomeIcon /> }, { 'label': 'Ukraine-Russia War', 'link': '/scenario1', icon: <SentimentVeryDissatisfiedIcon /> },
    { 'label': 'LGBTQ', 'link': '/scenario2', icon: <TransgenderIcon /> }, { 'label': 'Time', 'link': '/scenario3', icon: <BedtimeIcon /> }]
    const materialTheme = materialExtendTheme();
    return (
        <>
            <MaterialCssVarsProvider theme={{ [MATERIAL_THEME_ID]: materialTheme }}>
                <JoyCssVarsProvider>
                    <CssBaseline />
                    <AppBar
                        sx={{ width: `calc(100% - ${drawerWidth}px)`, ml: `${drawerWidth}px` }}>
                        <Toolbar>
                            <Typography variant="h4" noWrap component="div">
                                {title}
                            </Typography>
                        </Toolbar>
                    </AppBar>
                    <Drawer
                        sx={{
                            width: drawerWidth,
                            flexShrink: 0,
                            '& .MuiDrawer-paper': {
                                width: drawerWidth,
                                boxSizing: 'border-box',
                            },
                            color: "#c5eceb"
                        }}
                        variant="permanent"
                        anchor="left"
                    >
                        <Toolbar />
                        <Divider />

                        <List>
                            {navInfo.map((page, index) => (
                                <ListItem key={page.label} disablePadding>
                                    <ListItemButton component={NavLink} to={page.link}>
                                        <ListItemDecorator>
                                            {page.icon}
                                        </ListItemDecorator>
                                        <ListItemText   >{page.label}</ListItemText>
                                    </ListItemButton>
                                </ListItem>
                            ))}
                        </List>

                        <Divider />
                    </Drawer>
                </JoyCssVarsProvider>
            </MaterialCssVarsProvider>
        </>
    );
}