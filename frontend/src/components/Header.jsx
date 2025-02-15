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
            <img src={als_logo} alt="als logo" className="h-8 w-auto"/>
            <h1 className="text-4xl text-sky-900">GISAXS Data Viewer</h1>
            <img src={ml_logo} alt="bnl logo" className="h-8 w-auto"/>
            <img src={mwet_logo} alt="bnl logo" className="h-8 w-auto"/>
            <img src={illumine_logo} alt="bnl logo" className="h-8 w-auto"/>


        </header>
    )
}
