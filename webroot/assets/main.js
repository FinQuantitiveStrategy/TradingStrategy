
async function backtestfire(){
    let res = await fetch('/api/backtest-fire')
    console.log(await res.text())
}

async function triggerupdate() {
    let res = await fetch('/api/manual-update');
    console.log(await res.text())
}