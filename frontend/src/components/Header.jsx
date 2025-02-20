import Status from "./Status";
const als_logo = "/images/als_logo_wheel.png";
const ml_logo = "images/mlexchange_logo.png";
const mwet_logo = "images/mwet_logo.png";
const illumine_logo = "images/illumine_logo.png";


export default function Header({isExperimentRunning=false, showStatus=false, statusMessage=""}) {
    return(
        <header className="flex items-center h-8 py-8 justify-center space-x-4 shadow-lg relative">
            { showStatus &&
                <div className="absolute top-0 left-12 flex items-end">
                    <Status slideshow={isExperimentRunning}/>
                    <p className="text-sm font-light pl-2">{statusMessage}</p>
                </div>
            }
            <h1 className="text-4xl text-sky-900 px-8">GISAXS Data Viewer</h1>
            <img src={illumine_logo} alt="illumine logo" className="h-8 w-auto"/>
            <img src={ml_logo} alt="ml exchange logo" className="h-8 w-auto"/>
            <img src={mwet_logo} alt="mwet logo" className="h-8 w-auto"/>
        </header>
    )
}
