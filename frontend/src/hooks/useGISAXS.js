import { useEffect, useRef, useState } from 'react';
import msgpack from 'msgpack-lite';
import dayjs from 'dayjs';
import { getWsUrl } from '../utils/connectionHelper';
import { processAndDownsampleArrayData, processJSONPlot, updateCumulativePlot } from '../utils/plotHelper';

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
    }
};

export const useGISAXS = ({}) => {
    const [ messages, setMessages ] = useState([]);
    const [ currentArrayData, setCurrentArayData ] = useState([]);
    const [ cumulativeArrayData, setCumulativeArrayData ] = useState([]);
    const [ currentScatterPlot, setCurrentScatterPlot ] = useState([]);
    const [ cumulativeScatterPlots, setCumulativeScatterPlots ] = useState([]);
    const [ isExperimentRunning, setIsExperimentRunning ] = useState(false);
    const [ isReductionTest, setIsReductionTest ] = useState(false);
    const [ linecutYPosition, setLinecutYPosition ] = useState(50); //using 50 as a test, change default later

    const [ wsUrl, setWsUrl ] = useState(defaultWsUrl);
    const [ socketStatus, setSocketStatus ] = useState('closed');
    const [ frameNumber, setFrameNumber ] = useState();
    const [ warningMessage, setWarningMessage ] = useState('');
    const [ heatmapSettings, setHeatmapSettings ] = useState(defaultHeatmapSettings);
    const [ metadata, setMetadata ] = useState('');

    const ws = useRef(null);
    const isUserClosed = useRef(false);



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

            if (event.data instanceof Blob) {
                // Convert Blob to ArrayBuffer for binary processing
                const arrayBuffer = await event.data.arrayBuffer();
                newMessage = msgpack.decode(new Uint8Array(arrayBuffer));
                console.log({newMessage})

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
            }

            if ('curve' in newMessage) {
                const newPlot = processJSONPlot(newMessage['curve'], newMessage?.frame_number);
                setCurrentScatterPlot(newPlot);
                setCumulativeScatterPlots((prevState) => {
                    var newState = [...prevState];
                    newState.push(newPlot[0]);
                    return newState;
                });
                //updateCumulativePlot(newPlot, setCumulativeScatterPlots); //for use when showing all scatter plots on a single graph only
            }

            if ('raw_frame' in newMessage) {
                const maxArrayElements = 10000; //largest number of array elements we want to display in Plotly to avoid performance issues
                var downsampleFactor = Math.max(Math.sqrt(newMessage.raw_frame.length / maxArrayElements), 1);
                let newPlot = processAndDownsampleArrayData(newMessage.raw_frame,  newMessage.width, newMessage.height, downsampleFactor);
                if (downsampleFactor > 1) {
                    const width = newPlot[0].length;
                    const height = newPlot.length;
                    const elements = width * height;
                    console.log("Downsampled frame, new dimensions: " + width + " x " + height + " = " + elements);
                }
                console.log("newPlot dim1: " + newPlot.length);
                console.log("newPlot dim2: " + newPlot[0].length);
                console.log("original array length: " + newMessage.raw_frame.length);
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
    }

    const handleWebsocketClose = (event) => {
        ws.current = false;
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
    };

    const startWebSocket = () => {
        setWarningMessage('');

        ws.current = new WebSocket(wsUrl);

        ws.current.onopen = (event) => {
            setSocketStatus('Open');
            isUserClosed.current = false;
        }

        ws.current.onerror = (error) => {
            console.error('Error with ws: ' + error);
            setWarningMessage("Error connecting websocket: Check port/path and verify processor running");
        }

        ws.current.onmessage = (event) => {
            handleNewWebsocketMessages(event);
        };

        ws.current.onclose = (event) => {
            handleWebsocketClose(event);
        }
    };

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
        startWebSocket,
        closeWebSocket,
        heatmapSettings,
        handleHeatmapSettingChange,
        warningMessage,
        isReductionTest,
        metadata,
    }
}