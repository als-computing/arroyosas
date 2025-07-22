
import axios from 'axios';

export const getTiled = () => {
    //make an api call to static image as test. try to get the data out of it
    //http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::3,::3
}

export const authenticateTiled = () => {
    //TODO: fill out this function, it should make an api call to my nginx at /authenticate and then save the auth parameters to local storage
}

export const getSearchResults = async (searchPath, cb, mock = false) => {
    try {
        const response = await fetch(searchPath, {
            method: "GET",
            headers: {
                "Accept": "application/json",
            },
            cache: "force-cache",
        });
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
        // Detect if response is JSON or binary
        const contentType = response.headers.get("content-type");
        
        if (contentType.includes("application/json")) {
            return await response.json(); // Parse JSON
        } else if (contentType.includes("application/octet-stream")) {
            var rawBuffer = await response.arrayBuffer(); // Handle binary response
            var newArray = new Uint8Array(rawBuffer);
            return Array.from(newArray);
        } else {
            throw new Error("Unsupported response type");
        }
    } catch (error) {
        console.error('Error searching path: ', error);
    }
};

export const getTableData = async(url, cb) => {
    //valid final request url: http://127.0.0.1:8000/api/v1/table/partition/short_table?partition=0&format=application/json-seq
    //self link: http://127.0.0.1:8000/api/v1/metadata/short_table
    //partition link: http://127.0.0.1:8000/api/v1/table/partition/short_table?partition={index}
    try {
        let tableUrl ="";
        if (url.includes("metadata")) {
            //handle metadata link
            tableUrl = url.replace("metadata", "table/partition");
            tableUrl = tableUrl + '?partition=0&format=application/json-seq';
        } else {
            //handle valid link
            if (url.includes("&format=application/json-seq")) {
                tableUrl = url;
                //handle the partition link
            } else {
                tableUrl = url + '&format=application/json-seq';
            }
        }
        const response = await axios.get(tableUrl);
        const parsedData = response.data
            .trim() // Remove any extra newlines at start or end
            .split("\n") // Split by line
            .map((line) => JSON.parse(line)); // Parse each line as JSON

        //console.log(parsedData); // Now it's an array of objects
        // [{ A: 0.5699, B: 1.1398, C: 1.7098 }, ...]
        cb && cb(parsedData)
        return parsedData;
    } catch (error) {
        console.error('Error searching table data: ', error);
        return null;
    }
}


//const sampleTiledUrl = "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::1,::1"
/*    const response = await axios.get(searchPath);
//console.log({response});
//cb(response.data);
return response.data; */
//console.log({searchPath})