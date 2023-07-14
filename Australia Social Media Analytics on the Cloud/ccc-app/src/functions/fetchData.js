/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
// Fetch any JSON files

const baseUrl = 'http://45.113.234.176:5000'

export function getData(url, setData) {
    url = baseUrl + url
    fetch(url, {
        header: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    })
        .then((res) => res.json())
        .then((data) => {
            setData(data)
            console.log(data)
            console.log(`Fetch ${url} complete`)
        })
        .catch(err => {
            console.log('======failed to fetch data=======');
            console.log(err);
        });
}


// Fetch language sentiment data
export function getLangSentData(url, setData) {
    url = baseUrl + url

    fetch(url, {
        mode: 'cors',
        header: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    })
        .then((res) => res.json())
        .then((data) => Object.keys(data).map(key => {
            data[key].language = key
            return (data[key])
        })
        )
        .then((data) => {
            data.sort((a, b) => b.total - a.total)
            return (data)
        })
        .then((data) => {
            setData(data)
            console.log(data)
            console.log(`Fetch ${url} complete`)
        })
        .catch(err => {
            console.log('======failed to fetch data=======');
            console.log(err);
        })
}


// Fetch total war sentiment
export function getRUwarData(url, setData) {
    url = baseUrl + url

    fetch(url, {
        mode: 'cors',
        header: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    })
        .then((res) => res.json())
        .then((data) => Object.keys(data).map(key => ({ "sentiment_type": key, "count": data[key] })))
        .then((data) => {
            setData(data)
            console.log(data)
            console.log(`Fetch ${url} complete`)
        })
        .catch(err => {
            console.log('======failed to fetch data=======');
            console.log(err);
        })
}

// Fetch day and night count data (and calculate total)
export function getDayNightData(url, setData) {
    url = baseUrl + url

    fetch(url, {
        mode: 'cors',
        header: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    })
        .then((res) => res.json())
        .then((data) => Object.keys(data).map(key => (
            {
                "language": key,
                "day": data[key].day,
                "night": data[key].night
            }
        ))
        )
        .then((data) => {
            data.sort((a, b) => ((b.day + b.night) - (a.day + a.night)))
            return (data)
        })
        .then((data) => {
            setData(data)
            console.log(data)
            console.log(`Fetch ${url} complete`)
        })
        .catch(err => {
            console.log('======failed to fetch data=======');
            console.log(err);
        })
}