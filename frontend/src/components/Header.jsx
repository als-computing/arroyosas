import Status from "./Status";
const als_logo = "/images/als_logo_wheel.png";
const bnl_logo = "images/bnl_logo.png";


export default function Header({isExperimentRunning=false, showStatus=false}) {
    return(
        <header className="flex items-center h-8 py-8 justify-center space-x-4 shadow-lg relative">
            <div className="absolute top-0 left-12">
                {showStatus && <Status slideshow={isExperimentRunning}/>}
            </div>
            <img src={als_logo} alt="als logo" className="h-8 w-auto"/>
            <h1 className="text-4xl text-sky-900">SMI Live Data Viewer</h1>
            <img src={bnl_logo} alt="bnl logo" className="h-8 w-auto"/>
        </header>
    )
}
