/*
COMP90024 Project 2 2023
Contributor
Aobo Li              1172339
Pavith Samarakoon    1297058
Zhihao Liang         1367102
Jiqiang Chen         1171420
Yijun Liu            1132416
*/
export function round(x, nDecimal) {
    return (Math.round((x + Number.EPSILON) * (10**nDecimal)) / (10**nDecimal))
}