import { useEffect, useRef, useState } from 'react';

export default function Main({children}) {

    return (
        <main className="bg-slate-500 h-full flex-grow overflow-y-auto flex flex-wrap justify-around">
            {children}
      </main>
    )
}
