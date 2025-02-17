const images = [
    "images/xrayIcon/gif0.png",
    "images/xrayIcon/gif1.png",
    "images/xrayIcon/gif2.png",
    "images/xrayIcon/gif3.png",
    "images/xrayIcon/gif4.png",
  ];
  
  export default function Status({height='h-14', cycleTime=5, slideshow=true}) {
    return (
        <div className="flex justify-center mix-blend-multiply">
            <div className={`relative ${height} aspect-square`}>
                {
                    slideshow ?
                        images.map((src, index) => (
                            <img
                                key={index}
                                src={src}
                                alt="xray icon"
                                className={`absolute top-0 left-0 w-full h-full opacity-0 animate-fadeCycleSmooth`}
                                style={{ animationDelay: `${images.length/cycleTime * index * 1}s` }} // Stagger animation
                            />
                        ))
                    :
                        <img src={images[0]} className={`absolute top-0 left-0 w-full h-full opacity-60`}/>
                }
            </div>
        </div>
    );
  }
  