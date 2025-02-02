import { useEffect, useRef, useState } from 'react';

import msgpack from 'msgpack-lite';
import TextField from '../component_library/TextField';
import { getWsUrl } from '../utils/connectionHelper';
import dayjs from 'dayjs';


export const useAPXPS = ({}) => {

    const [ messages, setMessages ] = useState([]);


    const [ rawArray, setRawArray ] = useState([]);
    const [ vfftArray, setVfftArray ] = useState([]);
    const [ ifftArray, setIfftArray ] = useState([]);
    const [ shotRecentArray, setShotRecentArray ] = useState([]);
    const [ shotMeanArray, setShotMeanArray ] = useState([]);
    const [ shotStdArray, setShotStdArray ] = useState([]);
    const [ shotNumber, setShotNumber ] = useState(0);
    const [ shotInfo, setShotInfo ] = useState({}); //TO DO: put state into here to track shots to frames and use for tick marks

    const [ singlePeakData, setSinglePeakData ] = useState([]);
    const [ allPeakData, setAllPeakData ] = useState([]);

    const [ status, setStatus ] = useState({scan: 'N/A', websocket: 'N/A'});
    const [ metadata, setMetadata ] = useState({});

    const type = {
        float: 'float',
        integer: 'integer',
        string: 'string',
        enum: 'enum',
        file: 'file',
        boolean: 'boolean'
    };
    const defaultHeatmapSettings = {
        scaleFactor: {
            label: 'Scale Factor',
            type: type.float,
            value: '2',
            description: 'Factor to scale the vertical axis of Raw, VFFT, and IFFT images in the heatmap. Larger number will increase the vertical height.'
        },
        showTicks: {
            label: 'Tick Marks',
            type: type.boolean,
            value: false,
            description: 'Toggles the display of tickmarks on the heatmap graphs, where tickmarks represent the frame count at that row.'
        }
    };
    const [ heatmapSettings, setHeatmapSettings ] = useState(defaultHeatmapSettings);

    const handleHeatmapSettingChange = (newValue, key) => {
        setHeatmapSettings((prevState) => ({
            ...prevState,
            [key]: {
                ...prevState[key],
                value: newValue
            }
        }));
    };


    const frameNumber = useRef(null);

    const isUserClosed = useRef(null);



    const defaultWsUrl = getWsUrl();
    const [socketStatus, setSocketStatus] = useState('closed');
    const [ wsUrl, setWsUrl ] = useState(defaultWsUrl);
    const [frameCount, setFrameCount ] = useState('');
    const [timeStamp, setTimeStamp] = useState('');
    const [ warningMessage, setWarningMessage ] = useState('');
    const ws = useRef(null);

    const handleNewWebsocketMessages = async (event) => {
        //process with webpack and set to messages.
        try {
            let newMessage;

            if (event.data instanceof Blob) {
                // Convert Blob to ArrayBuffer for binary processing
                const arrayBuffer = await event.data.arrayBuffer();
                newMessage = msgpack.decode(new Uint8Array(arrayBuffer));
                //console.log('got a blob:');
                //console.log({newMessage});
            } else if (event.data instanceof ArrayBuffer) {
                // Process ArrayBuffer directly
                newMessage = msgpack.decode(new Uint8Array(event.data));
                //console.log('got array buffer:')
                //console.log({newMessage});

            } else {
                // Assume JSON string for non-binary data
                newMessage = JSON.parse(event.data);
                //console.log('got JSON:')
                //console.log({newMessage});
            }
            //log keys
            //console.log({newMessage})
            var keyList = '';
            for (const key in newMessage) {
                keyList = keyList.concat(', ', key);
            };

            setMessages((prevMessages) => [...prevMessages, keyList]);

            if ('frame_number' in newMessage) {
                //console.log({newMessage})
                frameNumber.current = newMessage.frame_number;
            }

            //handle fitted data parameters for line plots
            if ('fitted' in newMessage) {
                const fittedData = JSON.parse(newMessage.fitted);
                //console.log({fittedData})
                processPeakData(fittedData, setSinglePeakData, updateCumulativePlot)
            }

            //handle heatmap data
            if ('raw' in newMessage) {
                //console.log({newMessage})
                //send in height as width and vice versa until height/width issues fixed
                //processArrayData(newMessage.raw,  newMessage.height, newMessage.width, setRawArray);
                processAndDownsampleArrayData(newMessage.raw,  newMessage.height, newMessage.width, 2, setRawArray);
            }
            if ('vfft' in newMessage) {
                //console.log({newMessage})
                //send in height as width and vice versa until height/width issues fixed
                //processArrayData(newMessage.vfft, newMessage.height,  newMessage.width, setVfftArray);
                processAndDownsampleArrayData(newMessage.vfft,  newMessage.height, newMessage.width, 2, setVfftArray);
            }
            if ('ifft' in newMessage) {
                //console.log({newMessage})
                //send in height as width and vice versa until height/width issues fixed
                //processArrayData(newMessage.ifft, newMessage.height, newMessage.width, setIfftArray);
                processAndDownsampleArrayData(newMessage.ifft,  newMessage.height, newMessage.width, 2, setIfftArray);
            }

            if ('msg_type' in newMessage) {
                console.log({newMessage});
                //add to metadata display and clear cumulative plots
                handleStartDocument(newMessage);
            }
            if ('shot_recent' in newMessage) {
                //technically metadata won't have f_reset due to stale state at time of function initilization, need to put it into a ref
                var shotHeight = ("f_reset" in metadata) ? metadata.f_reset : (newMessage.shot_recent.length / newMessage.height)
                processArrayData(newMessage.shot_recent, newMessage.height, shotHeight, setShotRecentArray)
            }
            if ('shot_mean' in newMessage) {
                var shotHeight = ("f_reset" in metadata) ? metadata.f_reset : (newMessage.shot_mean.length / newMessage.height)
                processArrayData(newMessage.shot_mean, newMessage.height, shotHeight, setShotStdArray)
            }
            if ('shot_std' in newMessage) {
                var shotHeight = ("f_reset" in metadata) ? metadata.f_reset : (newMessage.shot_std.length / newMessage.height)
                processArrayData(newMessage.shot_std, newMessage.height, shotHeight, setShotMeanArray)
            }
            if ('shot_num' in newMessage) {
                setShotNumber(newMessage.shot_num);
            }
            console.log({newMessage})
        } catch (error) {
            console.error('Error processing WebSocket message:', error);
        }
    };

    const handleStartDocument = (msg) => {
        if (msg.msg_type === 'start') {
            setAllPeakData([]); //clear out cumulative peak data which was from a previous scan
        }
        setMetadata(msg);
    };

    const processArrayData = (data=[], width, height, cb) => {

        const newData = [];
        for (let i = 0; i < height; i++) {
            newData.push(data.slice(i * width, (i + 1) * width));
        }
        cb(newData);
    };


    const processAndDownsampleArrayData = (data = [], width, height, scaleFactor = 1, cb) => {
        if (scaleFactor < 1) throw new Error("Scale factor must be 1 or greater.");

        const downsampledHeight = Math.floor(height / scaleFactor);
        const downsampledWidth = Math.floor(width / scaleFactor);
        const newData = [];

        for (let row = 0; row < downsampledHeight; row++) {
            const newRow = [];
            for (let col = 0; col < downsampledWidth; col++) {
                let sum = 0;
                let count = 0;

                // Sum up values within the scaleFactor x scaleFactor block
                for (let i = 0; i < scaleFactor; i++) {
                    for (let j = 0; j < scaleFactor; j++) {
                        const originalRow = row * scaleFactor + i;
                        const originalCol = col * scaleFactor + j;
                        const index = originalRow * width + originalCol;

                        if (originalRow < height && originalCol < width) {
                            sum += data[index];
                            count++;
                        }
                    }
                }
                // Calculate the average value and add to the downsampled row
                newRow.push(sum / count);
            }
            newData.push(newRow);
        }
        cb(newData);
    };

    const processPeakData = (peakDataArray=[{x:0, h:0, fwhm: 0}], singlePlotCallback=()=>{}, multiPlotCallback=()=>{}) => {

        var recentPlots = [];
        peakDataArray.forEach(data => {
            //receives an array of objects
            var y_peak = data.h;
            var x_peak = data.x;

            // Calculate sigma and define x range
            var sigma = data.fwhm / (2 * Math.sqrt(2 * Math.log(2)));
            var x_min = x_peak - 5 * sigma;
            var x_max = x_peak + 5 * sigma;
            var step = (x_max - x_min) / 100;

            // Generate x and y values for the single plot
            var xValues = [];
            var yValues = [];
            for (let x = x_min; x <= x_max; x += step) {
                var y = y_peak * Math.exp(-Math.pow(x - x_peak, 2) / (2 * Math.pow(sigma, 2)));
                xValues.push(x);
                yValues.push(y);
            }

            // Create single plot object
            recentPlots.push({ x: xValues, y: yValues, type: 'scatter', mode: 'lines' });
        })


        //update state
        singlePlotCallback(recentPlots);
        multiPlotCallback(recentPlots);
    }



    const startWebSocket = () => {
        setWarningMessage('');

        ws.current = new WebSocket(wsUrl);

        ws.current.onopen = (event) => {
            setSocketStatus('Open');
            setStatus((oldState) => ({...oldState, ['websocket']: 'connected'}));
            isUserClosed.current = false;
        }

        ws.current.onerror = (error) => {
            console.log("error with ws");
            console.log({error});
            //alert("Unable to connect to websocket");
            setWarningMessage("Error connecting websocket: Check port/path and verify processor running");
        }

        ws.current.onmessage = (event) => {
            handleNewWebsocketMessages(event);
        };

        ws.current.onclose = (event) => {
            handleWebsocketClose(event);
        }
    };

    const handleWebsocketClose = (event) => {
        ws.current = false;
        setStatus((oldState) => ({...oldState, ['websocket']: 'disconnected', ['scan']: 'N/A'}));
        if (isUserClosed.current === true) {
            //do nothing, the user forced the websocket to close
            console.log('user closed websocket');
            return;
        } else {
            //if websocket closed due to external reason, send in warning and attempt reconnection
            const maxAttempt = 2;
            const time = 5; //time in seconds
            //alert(`Websocket ${event.currentTarget.url} closed at ${dayjs().format('h:mm:ss A')} `)
            console.log({event})

            // Attempt to reconnect
            setWarningMessage("WebSocket closed unexpectedly. Attempting to reconnect...");
            console.log(`WebSocket ${event.currentTarget.url} closed unexpectedly at ${dayjs().format('h:mm:ss A')}`);

            // Reconnection logic
            const maxAttempts = 2; // Number of attempts to reconnect
            let attempts = 0;

            const tryReconnect = () => {
                if (attempts >= maxAttempts) {
                    setWarningMessage("Failed to reconnect to WebSocket after multiple attempts.");
                    return;
                }
                if (ws.current !== false) {
                    //ws has restarted
                    return;
                } else {
                    attempts++;
                    console.log(`Reconnection attempt ${attempts}`);
                    startWebSocket();
                }
            };

            setTimeout(tryReconnect, time*1000);
        }
    }

    const closeWebSocket = () => {
        try {
            ws.current.close();
        } catch (error) {
            console.log({error});
            return;
        }
        setSocketStatus('closed');
        isUserClosed.current = true; //this function is only able to be called by the user
    };

    const updateCumulativePlot = (recentPlots) => {
       //console.log({frameNumber})
      setAllPeakData((data) => {
        var oldArrayData = Array.from(data);
        var newArrayData = [];
        let totalFrames = oldArrayData.length;
        let colorNumber = 255; //the lightest color for the oldest entries

        //TO DO: refactor this if its slowing the app down
        oldArrayData.forEach((plot, index) => {
            let colorWeight = (totalFrames - index) / totalFrames * colorNumber; //scale color based on index relative to total frames
            plot.line = {
                color: `rgb(${colorWeight}, ${colorWeight}, ${colorWeight})`,
                width: 1,
            };
            newArrayData.push(plot);
        })
        var newestData = [];
        recentPlots.forEach((plot) => {
            var newPlot = {
                x: plot.x,
                y: plot.y,
                line: {
                    color: 'rgb(0, 94, 245)',
                    width: 1,
                },
                name: `frame ${frameNumber.current ? frameNumber.current : 'NA'}`
            };
            newestData.push(newPlot);
        })
        return [...newArrayData, ...newestData];
      })
    };




    return {
        rawArray,
        vfftArray,
        ifftArray,
        singlePeakData,
        allPeakData,
        messages,
        wsUrl,
        setWsUrl,
        frameNumber,
        socketStatus,
        startWebSocket,
        closeWebSocket,
        warningMessage,
        status,
        heatmapSettings,
        handleHeatmapSettingChange,
        metadata,
        shotNumber,
        shotRecentArray,
        shotMeanArray,
        shotStdArray
    }
}
