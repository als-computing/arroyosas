export default function ConsoleViewer({messages=[]}){
    //we cant display raw data inthe viewer, it will crash browser. just use this for helpful messages if needed in the future
    return (
        <ul className="w-full overflow-y-auto h-full">
            {messages.map((message, index) => {
                return(
                    <li key={index}>
                        <p>{`${index} ${message}`}</p>
                    </li>
                )
            })}
        </ul>
    )
}
