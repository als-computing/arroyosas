export default function ScanMetadata({status={websocket:'disconnected', scan:'started'}, metadata={}}) {
    // Format metadata as a pretty JSON string
    const prettyMetadata = JSON.stringify(metadata, null, 2);

/*     {Object.keys(status).map((key) => {
        return (
            <li key={key} className="flex justify-start items-center">
                <div className={`rounded-full aspect-square w-4 h-4 animate-pulse border border-slate-300 mr-3 ${status[key].startsWith('connected') || status[key].startsWith('started') ? 'bg-green-500' : 'bg-red-400'}`}></div>
                <p>{`${key}: ${status[key]}`}</p>
            </li>
        )
    })} */
    return (
        <div className="flex-col w-full list-none">


            {/* Metadata display with JSON pretty formatting*/}
            <div className="w-full">
                <pre className="text-sm font-mono text-gray-700 whitespace-pre-wrap">{prettyMetadata}</pre>
            </div>
        </div>
    )
}
