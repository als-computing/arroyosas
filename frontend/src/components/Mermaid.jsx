import { useEffect, useState } from "react";
import Button from "../component_library/Button";
export default function Mermaid() {
    const [ sampleText, setSampleText ] = useState('');
    const chartItem = {
        name: 'viz_op',

    };

    //the service item can also start recording plots
    //how do we want to store plot data for each serviceItem?
    const serviceItem = {
        id: 'viz_op',
        nickname: 'Viz Operator',
        description: 'A visual operator that does something',
        runStatus: false, //used to change color of item in mermaid
        plotData: null, //displays a plot
        isClickable: true, //boolean for allowing cursor hover effects
    };

    //after the mermaid plot is created, create react state for each item?



    const graph = `
        flowchart LR
            style Mars1Group stroke:#bbb,stroke-width:4px,stroke-dasharray: 5,5,fill:none;
            
            btiled(Beamline Tiled) <--http--> framelisten(Frame Listener);
            
            subgraph Mars1Group["mars1.nsls2.bnl.gov"];
                framelisten --zmq frame--> viz_op(Viz Operator);
                framelisten --zmq frame + tiledurl--> lse_op(LSE Operator);
                lse_op --websocket--> lse(Latent Space Explorer dash);
        
                lse <--http--> browser(Browser);
                viz_op --http 1D reductions--> tiled(Tiled);
                viz_op --websocket 2 tiled urls + data--> browser;
                lse_op <--zmq raw frame--> lse_worker1(LSE Worker 1);
                lse_op <--zmq raw frame--> lse_worker2(LSE Worker 2);
                lse_op <--zmq raw frame--> lse_worker3(LSE Worker N);
                lse_op --http feature vec and url--> tiled;
        
                browser <--http get data--> tiled;
                tiled <----> postgres;
                wf_viz(Workflow Viz) <--http--> browser;
                wf_viz --> redis(Redis);
                redis --> viz_op;
        
        
            end;
    `


    const attachEventHandler = () => {
        const callBackFunction = (event) => {
            console.log("Clicked element:", event.currentTarget);
        };
    
        // Select all node elements
        const nodes = document.querySelectorAll("g.node");
    
        // Attach the event handler to each node
        nodes.forEach(node => {
            node.addEventListener("click", callBackFunction);
        });
    };

    const changeColorTest = () => {
        const element = getRectByNodeLabel("LSE Operator");
        if (element) changeBackgroundColor(element, "red");
        setSampleText('made it red')
        const newElement = getRectByParentId('lse');
        console.log({newElement})
        changeBackgroundColor(newElement, "blue")
    };

    const getRectByParentId = (text) => {
        // Select the parent <g> element by matching its ID pattern
        const parent = document.querySelector(`[id^="flowchart-${text}-"]`);
        if (parent) {
            const rect = parent.querySelector("rect");
            if (rect) return rect;
        }

        return null;
    };

    const getRectByNodeLabel = (searchText='') => {
        //The rectangles <rect> will have a sibling <g> that contains a child with the name in parenthesis of the item
        //ex) for: framelisten --zmq frame + tiledurl--> lse_op(LSE Operator);
        // the search text should be "LSE Operator"
        const labelText = searchText;
        const labelElement = Array.from(document.querySelectorAll(".nodeLabel"))
            .find(el => el.innerText.trim() === labelText);

        // The HTML structure is g->rect, where rect has sibling g->foreignObject->div->span => searchText
        if (labelElement) {
            const nodeGroup = labelElement.closest("g.node");
            if (nodeGroup) {
                const rect = nodeGroup.querySelector("rect");
                if (rect) {
                    return rect;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    };
    
    const changeBackgroundColor = (element, color="red") => {
        element.style.fill = color;
    }

    const getLineByEndpoints = (endpoint1, endpoint2) => {
        const examplePathId = "L-redis-viz_op-0" // connects redis to viz_op
        // the pattern is 'L- endpoint1 - endpoint2 -0'
        const idA = `#L-${endpoint1}-${endpoint2}-0`;
        const idB = `#L-${endpoint2}-${endpoint1}-0`;
        let path = false;
        path = document.querySelector(idA);
        if (path) return path;
        path = document.querySelector(idB);
        if (path) {
            return path;
        } else {
            return false;
        }

    }


    function new_script(src) {
        return new Promise(function(resolve, reject){
          if (typeof window !== "undefined") {
            var script = window.document.createElement('script');
            script.src = src;
            script.addEventListener('load', function () {
              resolve();
            });
            script.addEventListener('error', function (e) {
              reject(e);
            });
            window.document.body.appendChild(script);
          }
        })
      };

    useEffect(() => {
        if (!window.mermaid) {
            var my_script = new_script('https://cdnjs.cloudflare.com/ajax/libs/mermaid/9.3.0/mermaid.min.js');
            my_script.then(() => {
                window.mermaid.mermaidAPI.initialize({
                    securityLevel: 'loose',
                });
                window.mermaid.contentLoaded();
                attachEventHandler();
            });
        }   
    }, []);



    return (
        <>
        <div className="mermaid">
            {graph}
        </div>
        <div>
            <Button cb={changeColorTest} text="change to red"/>
            <p>{sampleText}</p>
        </div>
        
        </>
    )
}