import Mermaid from "../components/Mermaid";
import { getSearchResults } from "../utils/tiledAPI";
import PlotlyHeatMap from "../components/PlotlyHeatMap";
export default function Services(){
    const data = getSearchResults();
    return (
        <div className="w-full h-screen ">
            <PlotlyHeatMap />
        </div>
    )
}