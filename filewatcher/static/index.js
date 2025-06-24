const urlStartFilefatcher = 'http://localhost:8081/filewatcher/start'
const urlStopFilefatcher = 'http://localhost:8081/filewatcher/stop'
const urlGetState = 'http://localhost:8081/filewatcher/get_state'

const ws = new WebSocket('ws://localhost:8081/ws');

let timerElement = document.getElementById('timer-container')
let startButton = document.getElementById('start')
let stopButton = document.getElementById('stop')
let resetButton = document.getElementById('reset')
let statistics = document.getElementById('statistics')

ws.onopen = function(event) {
    console.log("Connected to WebSocket");
  };

ws.onmessage = function(event) {
    console.log("Message from server: ", event.data);
    updateUIBasedOnState(event.data);
};

function updateUIBasedOnState(state) {
    if (state) {
        startButton.disabled = true
        stopButton.disabled = false

        timerElement.style.color = 'green'
        timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВКЛЮЧЕНО</h1><h3>Дата и время следующего обновления: ${stateServer.date} 04:00</h3>`

        startButton.style.cursor = 'auto'
        startButton.style.filter = 'brightness(60%)'

        stopButton.style.cursor = 'pointer'
        stopButton.style.filter = 'brightness(100%)'
    } else {
        startButton.disabled = false
        stopButton.disabled = true
        
        timerElement.style.color = 'red'
        timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВЫКЛЮЧЕНО</h1>`
        
        stopButton.style.cursor = 'auto'
        stopButton.style.filter = 'brightness(60%)'

        startButton.style.cursor = 'pointer'
        startButton.style.filter = 'brightness(100%)'   
    }
}

startButton.addEventListener('click', () => {
    ws.send('start');
    stateServer = await getQuery(urlStartFilefatcher)
    startButton.disabled = true
    stopButton.disabled = false
    stateBtnServer = true

    timerElement.style.color = 'green'
    timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВКЛЮЧЕНО</h1><h3>Дата и время следующего обновления: ${stateServer.date} 04:00</h3>`

    startButton.style.cursor = 'auto'
    startButton.style.filter = 'brightness(60%)'
    startButton.style.boxShadow = ''

    stopButton.style.cursor = 'pointer'
    stopButton.style.filter = 'brightness(100%)'
  });
  
  stopButton.addEventListener('click', () => {
    ws.send('stop');

    startButton.disabled = false
    stopButton.disabled = true
    stateBtnServer = false
    
    timerElement.style.color = 'red'
    timerElement.innerHTML = '<h1>АВТООБНОВЛЕНИЕ ВЫКЛЮЧЕНО</h1>'
    
    stopButton.style.cursor = 'auto'
    stopButton.style.filter = 'brightness(60%)'
    stopButton.style.boxShadow = ''

    startButton.style.cursor = 'pointer'
    startButton.style.filter = 'brightness(100%)'
  });

async function getQuery(url) {
    return await fetch(url)
    .then(response => response.json())
    .then(json => json)
    .catch(error => console.log(error))
}

async function postQuery(url, object) {
    return await fetch(url, {
        method: 'POST',
        body: JSON.stringify(object),
        headers: {
            'Content-Type': 'application/json'
        }
    })
    .then(response => response.json())
    .then(json => console.log(json)).
    catch(error => console.log(error))
}

const setBtnShadow = (element, state) => {
    let color = window.getComputedStyle(element).backgroundColor
    if (state) {
        element.style.boxShadow = `0 0 0 2px white, 0 0 0 4px ${color}`
    } else {
        element.style.boxShadow = ''
    }
}

async function Home() {
    let stateServer = await getQuery(urlGetState)
    stateBtnServer = !!stateServer.state

    /*
    if (stateBtnServer) {
        startButton.disabled = true
        stopButton.disabled = false

        timerElement.style.color = 'green'
        timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВКЛЮЧЕНО</h1><h3>Дата и время следующего обновления: ${stateServer.date} 04:00</h3>`

        startButton.style.cursor = 'auto'
        startButton.style.filter = 'brightness(60%)'

        stopButton.style.cursor = 'pointer'
        stopButton.style.filter = 'brightness(100%)'
    } else {
        startButton.disabled = false
        stopButton.disabled = true
        
        timerElement.style.color = 'red'
        timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВЫКЛЮЧЕНО</h1>`
        
        stopButton.style.cursor = 'auto'
        stopButton.style.filter = 'brightness(60%)'

        startButton.style.cursor = 'pointer'
        startButton.style.filter = 'brightness(100%)'   
    }
    */
      
    /*
    startButton.addEventListener('click', async () => {
        ws.send('start');
        stateServer = await getQuery(urlStartFilefatcher)
        startButton.disabled = true
        stopButton.disabled = false
        stateBtnServer = true
    
        timerElement.style.color = 'green'
        timerElement.innerHTML = `<h1>АВТООБНОВЛЕНИЕ ВКЛЮЧЕНО</h1><h3>Дата и время следующего обновления: ${stateServer.date} 04:00</h3>`
    
        startButton.style.cursor = 'auto'
        startButton.style.filter = 'brightness(60%)'
        startButton.style.boxShadow = ''
    
        stopButton.style.cursor = 'pointer'
        stopButton.style.filter = 'brightness(100%)'
    })
    */
    startButton.addEventListener('mouseover', () => {
        if (!stateBtnServer) {
            setBtnShadow(startButton, true)
        }
    })
    
    
    startButton.addEventListener('mouseleave', () => {
        setBtnShadow(startButton, false)
    })
    
    /*
    stopButton.addEventListener('click', async () => {
        stateServer = await getQuery(urlStopFilefatcher)
        startButton.disabled = false
        stopButton.disabled = true
        stateBtnServer = false
        
        timerElement.style.color = 'red'
        timerElement.innerHTML = '<h1>АВТООБНОВЛЕНИЕ ВЫКЛЮЧЕНО</h1>'
        
        stopButton.style.cursor = 'auto'
        stopButton.style.filter = 'brightness(60%)'
        stopButton.style.boxShadow = ''
    
        startButton.style.cursor = 'pointer'
        startButton.style.filter = 'brightness(100%)'
    })*/
    
    stopButton.addEventListener('mouseover', () => {
        if (stateBtnServer) {
            setBtnShadow(stopButton, true)
        }
    })
    
    stopButton.addEventListener('mouseleave', () => {
        setBtnShadow(stopButton, false)
    })
    
    resetButton.addEventListener('click', () => {
        startButton.disabled = false
    })
    
    resetButton.addEventListener('mouseover', () => {
        setBtnShadow(resetButton, true)
    })
    
    resetButton.addEventListener('mouseleave', () => {
        setBtnShadow(resetButton, false)
    })
}

Home()