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
  Typography, Card,
  CardContent, Grid,
  Container
} from "@mui/material";
import PageTitle from "../components/PageTitle";
import { getData, getDayNightData } from "../functions/fetchData";
import BarLangDayNight from "../components/time/BarLangDayNight";
import BarLangDayNightSortLang from "../components/time/BarLangDayNightSortLang";
import ChartTitle from "../components/ChartTitle";
import LinesHourly from "../components/time/LinesHourly";
import LinesHourlyNormalised from "../components/time/LinesHourlyNormalised";
import Skeleton from '@mui/material/Skeleton';

function Scenario3({ stateData, suburbData, updateTitle,title }) {

  const [mastHourly, setMastHourly] = useState(null)
  const [mastDayNight, setMastDayNight] = useState(null)
  const [mastLangHourly, setMastLangHourly] = useState(null)
  const [mastLangDayNight, setMastLangDayNight] = useState(null)
  const [twitterHourly, setTwitterHourly] = useState(null)
  const [twitterDayNight, setTwitterDayNight] = useState(null)
  const [twitterStateHourly, setTwitterStateHourly] = useState(null)
  const [twitterLangHourly, setTwitterLangHourly] = useState(null)
  const [twitterLangDayNight, setTwitterLangDayNight] = useState(null)

  useEffect(() => {
    document.title = title;
    updateTitle("Cluster and Cloud Computing - Team 14: Time")
    getData('/general/mastondon/count_hourly', setMastHourly)
    getData('/general/mastondon/count_day_night', setMastDayNight)
    //getData('/general/mastodon/language_hour', setMastLangHourly)
    getDayNightData('/general/mastodon/language_day_night', setMastLangDayNight)
    getData('/general/twitter/state_hourly_tweet', setTwitterStateHourly)
    //getData('/general/twitter/language_hour', setTwitterLangHourly)
    getDayNightData('/general/twitter/language_day_night', setTwitterLangDayNight)
  }, [])

  return (
    <>
      <Container style={{ marginTop: '2em', display: 'flex' }}>
        <Grid container spacing={2} >
          {

            <>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full" >
                  <CardContent>
                    {(twitterStateHourly && mastHourly) ? <LinesHourly data={twitterStateHourly} mastData={mastHourly} /> : <Skeleton variant="rectangular" width={700} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent>
                    <Typography variant="b1">
                      <b>Mastodon acitivy and State Twitter activity</b><br />
                      Twitter activity across states show a similar trend, highest activity during 6-9pm and very little activity between 12-5am,
                      as expected since this corresponds with when most people have finished work/school and have more free time to be active
                      on social media, and also a drop in activity during the time when most people are sleeping. Victoria and NSW had the most
                      activity by a large margin, due to having the largest populations. Mastodon shows a very different trend (note all times are in AEST),
                      with highest activity during 11pm-6am, when australians would be sleeping, and in strong contrast with twitters activity
                      for australians, suggesting that there is likely a large proportion of international users active in this australian mastodon server.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </>

          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent>
                    <Typography variant="b1">
                      <b>Normalised activity</b><br />
                      The normalised plot further indicates the similar trend in least and most active periods across states for twitter,
                      with each state shifted across time due to the timezone differences (times shown in AEST). However, NT has an
                      unexpected peak in activity at ~4pm AEST and relatively lower activity at 7-8pm when other states are most active.
                      Mastodon's normalised activity goes against the pattern seen on twitter, as was expected after the analysis
                      of the above unnormalised plot.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full">
                  <CardContent>
                    {(twitterStateHourly && mastHourly) ? <LinesHourlyNormalised data={twitterStateHourly} mastData={mastHourly} /> : <Skeleton variant="rectangular" width={700} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
            </>

          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent className="flex  justify-center">
                    {mastLangDayNight ? <div> <ChartTitle title="Mastodon Day/Night (sort Total)" />
                      <BarLangDayNight data={mastLangDayNight.slice(1)} /></div> :
                      <Skeleton variant="rectangular" width={300} height={350} />}


                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent>
                    <Typography variant="b1">
                      <b>Time (Day night)</b><br />
                      Languages, excluding English is analysed based on the time tweet/toot is posted. Japanese is the language
                      that tweeted the most on Twitter, whereas German is the most frequent languages of Mastodon's toots.
                      Spanish and Chinese top the list of posts on both social media, which is understandable given the popularity
                      of both languages. Generally speaking, Mastodon has a higher proportion of toots posted during night time (10pm-5am)
                      compare to Twitter. There are two possible reason for this finding. Mastodon is a new social media platform
                      launched in 2016, so there might be a higher proportion of young users who stayed up late. Another possible
                      reason is that, Mastodon do not contain post location information, hence anyone around the world could post
                      at Australia's server, which could cause this misleading result.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </>
          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full ">
                  <CardContent className="flex  justify-center   ">
                    {(twitterLangDayNight) ? <div>  <ChartTitle title="Twitter Day/Night (sort Total)" />
                      <BarLangDayNight data={twitterLangDayNight.slice(2)} /> </div> : <Skeleton variant="rectangular" width={300} height={350} />}
                    {/* Remove english and undefined with slice */}
                  </CardContent>
                </Card>
              </Grid>
            </>
          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent className="flex  justify-center   ">
                    {mastLangDayNight ? <div>       <ChartTitle title="Mastodon Day/Night (sort Language)" />
                      <BarLangDayNightSortLang data={mastLangDayNight.slice(1)} /> {/* Remove english with slice */}</div>
                      : <Skeleton variant="rectangular" width={300} height={350} />
                    }


                  </CardContent>
                </Card>
              </Grid>
            </>
          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full">
                  <CardContent className="flex  justify-center content-center " >

                    {(twitterLangDayNight) ? <div>      <ChartTitle title="Twitter Day/Night (sort Language)" />
                      <BarLangDayNightSortLang data={twitterLangDayNight.slice(2)} /> {/* Remove english and undefined with slice */}</div> : <Skeleton variant="rectangular" width={300} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
            </>
          }
        </Grid>
      </Container>
    </>
  );
}

export default Scenario3;
