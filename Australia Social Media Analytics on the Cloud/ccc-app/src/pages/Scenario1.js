/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
import React, { useEffect, useState } from "react";
import Scenario1Map from "../components/war/Scenario1Map";

import {
  Typography, Card,
  CardContent, Grid,
  Container
} from "@mui/material";
import PageTitle from "../components/PageTitle";
import PieTotalSent from "../components/PieTotalSent";
import { getData, getLangSentData, getRUwarData } from "../functions/fetchData";
import PieTotalWarSent from "../components/common/PieTotalSent";
import LinesStateMonth from "../components/common/LinesStateMonth";
import BarTwitterLangSent from "../components/common/BarTwitLangSent";
import BarMastLangSent from "../components/common/BarMastLangSent";
import LinesStateMonthMag from "../components/common/LinesStateMonthMag";
import ScatterWarIncEdu from "../components/war/ScatterWarIncEdu";
import ScatterwarElectPartdiff from "../components/war/ScatterWarElecPartdiff";
import Skeleton from '@mui/material/Skeleton';


function Scenario1({ stateData = null, suburbData = null, landSent = null, updateTitle, title }) {

  const [stateMonthly, setStateMonthly] = useState(null)
  const [ruWar, setRUwar] = useState(null)
  const [twitterLangSent, setTwitterLangSent] = useState(null)
  const [mastLangSent, setMastLangSent] = useState(null)
  const [mastProp, setMastProp] = useState(null)
  const [twitterProp, setTwitterProp] = useState(null)

  useEffect(() => {
    document.title = title;
    updateTitle("Cluster and Cloud Computing - Team 14: Ukraine-Russia War")

    getData('/war/twitter/monthly_state_sentiment', setStateMonthly)
    getData('/war/mastondon/proportion_sentiment', setMastProp)
    getData('/war/twitter/total_sentiment', setTwitterProp)
    getLangSentData('/war/twitter/sentiment_language', setTwitterLangSent)
    getLangSentData('/war/mastondon/sentiment_lang', setMastLangSent)
  }, [])

  return (
    <>
      <Container xs={12} md={12} lg={12} style={{ marginTop: '0' }}>
        <Scenario1Map stateData={stateData} suburbData={suburbData} />
      </Container>
      <Container style={{ marginTop: '2em', display: 'flex' }}>
        <Grid container spacing={2} >
          {

            <>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    {/* loading */}
                    {(stateMonthly) ? <LinesStateMonth data={stateMonthly} /> : <Skeleton variant="rectangular" width={700} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent className="flex justify-center " >
                    <Typography variant="b1">
                      <b>Difference in pos/neg:</b><br />
                      In the graph of comparing positive-negative sentiment differences, it is evident that sentiment of almost
                      all states between February 2022 and August 2022 has a tendency of negative emotions when discussing Russian
                      and Ukraine war. Among all the states, Tasmania stands out with the highest proportion of negative negative
                      tweets over 7 months, while South Australia seem to be a bit more fluctuating.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <Typography variant="b1">
                      <b>Magnitude of sentiment:</b><br />
                      Average magnitude of sentiment on the Russian Ukraine war over 8 states and territories is relatively stable.
                      Western Australia seem to be the state with highest average sentiment magnitude over the months, whereas
                      Northern Territory started off with a high average magnitude of sentiment and then became calm over the months.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    {/* loading */}
                    {(stateMonthly) ? <LinesStateMonthMag data={stateMonthly} /> : <Skeleton variant="rectangular" width={700} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
            </>
          }
          {

            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    {/* loading */}
                    {(twitterLangSent) ? <BarTwitterLangSent data={twitterLangSent} /> : <Skeleton variant="rectangular" width={300} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    {/* loading */}
                    {(mastLangSent) ? <BarMastLangSent data={mastLangSent} /> : <Skeleton variant="rectangular" width={300} height={350} />}

                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <Typography variant="b1">
                      <b>Sentiment proportion:</b><br />
                      The plot on the left shows the proportion of sentiment for the top 10 languages posted on twitter and mastodon.
                      We could spot that mastodon users seem to be more positive towards Russian Ukraine war. Possible reason for this
                      outcome is, Twitter's tweets were collected since the war had just started, however mastodon toots are streamed
                      recently. Ukraine has achieved more victories on the battlefield after receiving help from Europe and the United
                      States, therefore people posting toots more positively on these victories.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </>

          }
          {
            mastProp && (twitterProp &&
              <>
                <Grid item xs={12} md={6} lg={3}>
                  <Card className="h-full flex justify-center  " >
                    <CardContent>
                      <Typography variant="b1">
                        <b>Overall sentiment</b><br />
                        Overall sentiment comparison between tweets and toots also follows a similar pattern. User's emotion towards
                        Russian Ukraine war has becoming more positive due to the increasing victories. This is possibly something we
                        should expect to see if we have recent tweets for analysis.
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={12} lg={9}>
                  <Card className="h-full flex justify-center  " >
                    <CardContent>
                      <PieTotalWarSent mastProp={mastProp} twitterProp={twitterProp} />
                    </CardContent>
                  </Card>
                </Grid>
              </>
            )
          }
          {
            suburbData != null &&
            <>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <ScatterWarIncEdu data={suburbData} />
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <Typography variant="b1">
                      <b>Median Income vs proportion of negative sentiment</b><br />
                      In the graph comparing median income with proportion of negative sentiment, there seem to have some
                      relation between education level, median income and sentiment.
                      Suburbs with more than 25% of residents holding a Bachelor degree or above has a mean of about $53,000
                      median income, with a proportion of negative sentiment concentrated at around 0.6. Suburbs with less than
                      25% of residents holding a Bachelor degree or above has an average of about $39,000 median income, while also
                      have a more disperse distribution of negative sentiment proportion.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </>
          }
          {
            suburbData != null &&
            <>
              <Grid item xs={12} md={6} lg={4}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <Typography variant="b1">
                      <b>Male-Female Emp % diff vs Negative sentiment%</b><br />
                      Correlation between Male-Female employment difference against negative sentiment for each suburb with their polling
                      result is hard to interpret. It seems that Male-Female employment difference might have an influence on suburbs to
                      vote for Liberal, however the influence is too small to distinguish between Liberal and Labor votes. Percentage of
                      Negative sentiment on Russia and Ukraine war also seem to have no direct link with Suburb's polling result.
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={12} lg={8}>
                <Card className="h-full flex justify-center  " >
                  <CardContent>
                    <ScatterwarElectPartdiff data={suburbData} />
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

export default Scenario1;
