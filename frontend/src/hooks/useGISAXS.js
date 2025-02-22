import { useEffect, useRef, useState } from 'react';
import msgpack from 'msgpack-lite';
import dayjs from 'dayjs';
import { getWsUrl } from '../utils/connectionHelper';
import { processAndDownsampleArrayData, processJSONPlot, flip2DArray, updateCumulativePlot, process1DArray } from '../utils/plotHelper';

const defaultWsUrl = getWsUrl();
const defaultHeatmapSettings = {
    tickStep: {
        label: 'Tick Step',
        type: 'float',
        value: '10',
        description: 'Factor to scale the vertical axis of Raw, VFFT, and IFFT images in the heatmap. Larger number will increase the vertical height.'
    },
    showTicks: {
        label: 'Tick Marks',
        type: 'boolean',
        value: false,
        description: 'Toggles the display of tickmarks on the heatmap graphs, where tickmarks represent the frame count at that row.'
    },
    flipImg: {
        label: 'Flip Image',
        type: 'boolean',
        value: true,
        description: 'Adjusts the Plotly heatmap display to flip the image over the horizontal axis'
    }
};

export const useGISAXS = ({}) => {
    const [ messages, setMessages ] = useState([]);
    const [ currentArrayData, setCurrentArayData ] = useState([]);
    const [ cumulativeArrayData, setCumulativeArrayData ] = useState([]);
    const [ currentScatterPlot, setCurrentScatterPlot ] = useState([]);
    const [ cumulativeScatterPlots, setCumulativeScatterPlots ] = useState([]);
    const [ tiledLinks, setTiledLinks ] = useState([]);
    const [ isExperimentRunning, setIsExperimentRunning ] = useState(false);
    const [ isReductionTest, setIsReductionTest ] = useState(false);
    const [ linecutYPosition, setLinecutYPosition ] = useState(50); //using 50 as a test, change default later
    const [ linecutData, setLinecutData ] = useState(null);

    const [ wsUrl, setWsUrl ] = useState(defaultWsUrl);
    const [ socketStatus, setSocketStatus ] = useState('closed');
    const [ socketHistory, setSocketHistory ] = useState('No ws status');
    const [ frameNumber, setFrameNumber ] = useState(0);
    const [ warningMessage, setWarningMessage ] = useState('');
    const [ heatmapSettings, setHeatmapSettings ] = useState(defaultHeatmapSettings);
    const [ metadata, setMetadata ] = useState('');

    const ws = useRef(null);
    const isUserClosed = useRef(false);
    const reconnectionAttempts = useRef(0);
    const websocketMessageCount = useRef(0);




    const handleHeatmapSettingChange = (newValue, key) => {
        setHeatmapSettings((prevState) => ({
            ...prevState,
            [key]: {
                ...prevState[key],
                value: newValue
            }
        }));
    };

    const handleNewWebsocketMessages = async (event) => {
        //process with webpack
        try {
            let newMessage;
            let timestamp = dayjs().format('h:m:s a');
            setSocketHistory('Last ws message at: ' + timestamp);
            if (event.data instanceof Blob) {
                // Convert Blob to ArrayBuffer for binary processing
                const arrayBuffer = await event.data.arrayBuffer();
                newMessage = msgpack.decode(new Uint8Array(arrayBuffer));
                if (websocketMessageCount.current < 10) {
                    console.log({newMessage});
                    websocketMessageCount.current = websocketMessageCount.current + 1;
                } else {
                    if (websocketMessageCount.current === 10) {
                        console.log('Exceeded allowable message prints, suppressing future websocket messages');
                        websocketMessageCount.current = websocketMessageCount.current + 1;
                    }
                }/* 
                var websocketMessage = {}
                for (var key in newMessage) {
                    if (key === 'raw_frame') {
                        websocketMessage['raw_frame'] = 'data (too long to print)';
                    } else {
                        if (key === 'curve') {
                            websocketMessage['curve'] = 'data (too long to print)'
                        } else {
                            websocketMessage[key] = newMessage[key];
                        }

                    }
                }
                console.log({websocketMessage})
                 */
            } else if (event.data instanceof ArrayBuffer) {
                // Process ArrayBuffer directly
                newMessage = msgpack.decode(new Uint8Array(event.data));

            } else {
                // Assume JSON string for non-binary data
                newMessage = JSON.parse(event.data);
                console.log(newMessage);
            }
            var keyList = '';
            for (const key in newMessage) {
                keyList = keyList.concat(', ', key);
            };

            setMessages((prevMessages) => [...prevMessages, keyList]);

            if ('frame_number' in newMessage) {
                setFrameNumber(newMessage.frame_number);
            } else {
                setFrameNumber((prev)=> prev+1);
            }

            if ('curve' in newMessage) {
                //const newPlot = processJSONPlot(newMessage['curve'], newMessage?.frame_number);
                const newPlot = process1DArray(newMessage['curve'], newMessage?.frame_number);
                setCurrentScatterPlot(newPlot);
                setCumulativeScatterPlots((prevState) => {
                    var newState = [...prevState];
                    newState.push(newPlot[0]);
                    return newState;
                });
                //updateCumulativePlot(newPlot, setCumulativeScatterPlots); //for use when showing all scatter plots on a single graph only
            }

            if ('raw_frame' in newMessage) {
                const maxArrayElements = 90000000; //largest number of array elements we want to display in Plotly to avoid performance issues
                var downsampleFactor = Math.max(Math.sqrt(newMessage.raw_frame.length / maxArrayElements), 1);
                let newPlot = processAndDownsampleArrayData(newMessage.raw_frame,  newMessage.width, newMessage.height, downsampleFactor);
                if (downsampleFactor > 1) {
                    const width = newPlot[0].length;
                    const height = newPlot.length;
                    const elements = width * height;
                    console.log("Downsampled frame, new dimensions: " + width + " x " + height + " = " + elements);
                }
                try{
                    //log the max, min, and mean of raw frame
                    if (websocketMessageCount.current < 10) {
                        var maxValue = Math.max(...newMessage.raw_frame);
                        var minValue = Math.min(...newMessage.raw_frame);
                        var meanValue = newMessage.raw_frame.reduce((sum, value) => sum + value, 0) / arr.length;
                        console.log(`Values straight from ws: Max: ${maxValue}, Min: ${minValue}, Average: ${meanValue}`);
                        var maxValue = Math.max(...newMessage.raw_frame);
                        var minValue = Math.min(...newMessage.raw_frame);
                        var meanValue = newMessage.raw_frame.reduce((sum, value) => sum + value, 0) / arr.length;
                        console.log(`Values from plotly data: Max: ${maxValue}, Min: ${minValue}, Average: ${meanValue}`);
                    }
                } catch(e) {
                    console.error('issue processing arrays: ', e);
                }
                setCurrentArayData(newPlot);
                setCumulativeArrayData((prevState) => {
                    var newState = [...prevState];
                    var newPlotObject = {
                        data: newPlot,
                    };
                    try {
                        newPlotObject.metadata= {
                            timestamp: timestamp,
                            height: newMessage.height,
                            width: newMessage.width,
                            tiledUrl: newMessage.tiled_url,
                            dataType: newMessage.data_type,
                        }
                    } catch (e) {
                        console.error('Check keys in raw frame message: ', e);
                    }
                    newState.push(newPlotObject);
                    return newState;
                });
            };

            if ('raw_frame_tiled_url' in newMessage) {
                const links = {
                    image: newMessage.raw_frame_tiled_url,
                    curve: newMessage?.curve_tiled_url,
                    timestamp: timestamp
                };
                setTiledLinks((prevState) => [...prevState, links]);
            }

            if ('linecut' in newMessage) {
                //expected dictionary in websocket message
/*                 var linecut = {
                    x_min: 'integer',
                    x_max: 'integer',
                    cut_pos_y: 'integer',
                    cut_half_width: 'integer'
                } */
                const parameters = newMessage.linecut;
                try {
                    var linecut = {
                        x0: parameters.x_min,
                        x1: parameters.x_max,
                        y0: parameters.cut_pos_y, //assumed it is always a flat horizontal line
                        y1: parameters.cut_pos_y,
                        thickness: parameters.cut_half_width*2
                    };
                    setLinecutData(linecut);
                } catch (e) {
                    console.error('Error formatting linecut data from ws message: ', e);
                    console.log({parameters});
                }

                //replace this with the appropriate values from myLinecut
                const linecutSample = {
                    xStart: 0,
                    xEnd: 5,
                    yStart: 60,
                    yEnd: 70
                };



            }

            if ('msg_type' in newMessage) {
                if (newMessage.msg_type === 'start') {
                    resetAllData();
                    setIsExperimentRunning(true);
                    
                }
                if (newMessage.msg_type === 'stop') {
                    setIsExperimentRunning(false);
                }
                if (newMessage.scan_type === 'reduction') {
                    setIsReductionTest(true);
                } else {
                    setIsReductionTest(false);
                }
                setMetadata(newMessage);
            }
        } catch (error) {
            console.error('Error processing WebSocket message:', error);
        }
    };

    const resetAllData = () => {
        setCumulativeArrayData([]);
        setCumulativeScatterPlots([]);
        setCurrentArayData([]);
        setCurrentScatterPlot([]);
        setTiledLinks([]);
        setFrameNumber(0);
    }

    const handleWebsocketClose = (event) => {
        let timestamp = dayjs().format('h:m:s a');
        setSocketHistory('ws closed at: ' + timestamp);
        ws.current = false;
        setSocketStatus('closed');
        if (isUserClosed.current === true) {
            //do nothing, the user forced the websocket to close
            console.log('user closed websocket');
            return;
        } else {
            //if websocket closed due to external reason, send in warning and attempt reconnection
            const time = 5; //time in seconds
            console.log({event})
            // Attempt to reconnect
            setWarningMessage("WebSocket closed unexpectedly. Attempting to reconnect...");
            console.log(`WebSocket ${event.currentTarget.url} closed unexpectedly at ${dayjs().format('h:mm:ss A')}`);

            // Reconnection logic
            const maxAttempts = 5; // Number of attempts to reconnect

            const tryReconnect = () => {
                if (reconnectionAttempts.current >= maxAttempts) {
                    setWarningMessage("Failed to reconnect to WebSocket after multiple attempts.");
                    return;
                }
                if (ws.current !== false) {
                    //ws has restarted
                    return;
                } else {
                    reconnectionAttempts.current = reconnectionAttempts.current + 1;
                    console.log(`Reconnection attempt ${reconnectionAttempts.current}`);
                    startWebSocket();
                }
            };

            setTimeout(tryReconnect, time*1000);
        }
    };

    const startWebSocket = () => {
        let timestamp = dayjs().format('h:m:s a');
        setSocketHistory('attempting ws open at: ' + timestamp);
        setWarningMessage('');
        setSocketStatus('connecting');

        ws.current = new WebSocket(wsUrl);

        ws.current.onopen = (event) => {
            let timestamp = dayjs().format('h:m:s a');
            setSocketHistory('Opened ws at: ' + timestamp);
            setSocketStatus('Open');
            isUserClosed.current = false;
        }

        ws.current.onerror = (error) => {
            console.error('Error with ws: ' + error);
            let timestamp = dayjs().format('h:m:s a');
            setWarningMessage("Connection Error at " + timestamp);
        }

        ws.current.onmessage = (event) => {
            handleNewWebsocketMessages(event);
        };

        ws.current.onclose = (event) => {
            handleWebsocketClose(event);
        }
    };

    const closeWebSocket = () => {
        isUserClosed.current = true; //this function is only able to be called by the user
        try {
            ws.current.close();
        } catch (error) {
            console.log({error});
            return;
        }
        setSocketStatus('closed');
    };



    return {
        messages,
        currentArrayData,
        currentScatterPlot,
        cumulativeScatterPlots,
        cumulativeArrayData,
        isExperimentRunning,
        linecutYPosition,
        wsUrl,
        setWsUrl,
        frameNumber,
        socketStatus,
        socketHistory,
        startWebSocket,
        closeWebSocket,
        heatmapSettings,
        handleHeatmapSettingChange,
        warningMessage,
        isReductionTest,
        metadata,
        linecutData,
        tiledLinks
    }
}