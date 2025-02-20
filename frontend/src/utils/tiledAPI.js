
import axios from 'axios';

export const getTiled = () => {
    //make an api call to static image as test. try to get the data out of it
    //http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::3,::3
}

export const getSearchResults = async (searchPath, cb, mock = false) => {
    try {
        const sampleTiledUrl = "http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=158,::1,::1"
/*         const response = await axios.get(searchPath);
        //console.log({response});
        //cb(response.data);
        return response.data; */
        const response = await fetch(searchPath, {
            method: "GET",
            cache: "force-cache", // Ensures the request uses cached data if available
        });
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('Error searching path: ', error);
    }
};