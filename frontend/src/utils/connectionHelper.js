/**
 * Determines the path and port to WebSocket based on available env variables.
 * @returns {string} The full url path to WS.
 */
const getWsUrl = () => {
    const currentWebsiteIP = window.location.hostname;
    const pathname = "/viz"; 
    const port = ":8083";
    var wsUrl;

    const wsProtocol = window.location.protocol === "https:"
    ? "wss://"
    : "ws://";

    if (process.env.REACT_APP_WEBSOCKET_URL) {
        wsUrl = process.env.REACT_APP_WEBSOCKET_URL; //defined at top level .env file, accessed at build time
    } else {
        wsUrl = wsProtocol + currentWebsiteIP + port + pathname; //default when ran locally
    }

    return wsUrl;
}

export { getWsUrl };
