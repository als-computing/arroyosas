export default function JSONPrinter({JSON={}}) {
    return <pre className="text-sm font-mono text-gray-700 whitespace-pre-wrap break-words pl-4">{JSON.stringify(JSON, null, 2)}</pre>
}