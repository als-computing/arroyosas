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
    const [ currentScatterPlot, setCurrentScatterPlot ] = useState([]);
    const [ cumulativeScatterPlots, setCumulativeScatterPlots ] = useState([]);

    const [ wsUrl, setWsUrl ] = useState(defaultWsUrl);
    const [ socketStatus, setSocketStatus ] = useState('closed');
    const [ frameNumber, setFrameNumber ] = useState();
    const [ warningMessage, setWarningMessage ] = useState('');
    const [ heatmapSettings, setHeatmapSettings ] = useState(defaultHeatmapSettings);

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

            if (event.data instanceof Blob) {
                // Convert Blob to ArrayBuffer for binary processing
                const arrayBuffer = await event.data.arrayBuffer();
                newMessage = msgpack.decode(new Uint8Array(arrayBuffer));
                //console.log({newMessage})

            } else if (event.data instanceof ArrayBuffer) {
                // Process ArrayBuffer directly
                newMessage = msgpack.decode(new Uint8Array(event.data));

            } else {
                // Assume JSON string for non-binary data
                newMessage = JSON.parse(event.data);

            }
            var keyList = '';
            for (const key in newMessage) {
                keyList = keyList.concat(', ', key);
            };

            setMessages((prevMessages) => [...prevMessages, keyList]);

            if ('frame_number' in newMessage) {
                setFrameNumber(newMessage.frame_number);
            }

            //handle fitted data parameters for line plots
            if ('1D' in newMessage) {
                const newPlot = processJSONPlot(newMessage['1D'], newMessage?.frame_number);
                setCurrentScatterPlot(newPlot);
                updateCumulativePlot(newPlot, setCumulativeScatterPlots);
            }

            //handle heatmap data
            if ('image' in newMessage) {
                var width;
                if ('with' in newMessage) {
                    width = newMessage.with;
                } else if ('width' in newMessage) {
                    width = newMessage.width;
                }
                processAndDownsampleArrayData(newMessage.image,  width, newMessage.height, 1, setCurrentArayData);
            }
        } catch (error) {
            console.error('Error processing WebSocket message:', error);
        }
    };

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
        wsUrl,
        setWsUrl,
        frameNumber,
        socketStatus,
        startWebSocket,
        closeWebSocket,
        heatmapSettings,
        handleHeatmapSettingChange,
        warningMessage
    }
}