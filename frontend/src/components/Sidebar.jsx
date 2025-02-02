import React, { useState } from 'react';
const hamburgerIcon = <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
<path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" />
</svg>


export default function Sidebar({ children }) {
    const [isCollapsed, setIsCollapsed] = useState(false);

    const toggleSidebar = () => {
        setIsCollapsed(!isCollapsed);
    };

    return (
        <aside
            className={`bg-slate-200 flex-shrink-0 shadow-md h-full transition-all duration-300 overflow-auto ${
                isCollapsed ? 'w-[60px]' : 'w-[300px]'
            }`}
        >
            <div className="flex items-center justify-between p-2">
                <button
                    onClick={toggleSidebar}
                    className="p-2 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                    <div>
                        {hamburgerIcon}
                    </div>
                </button>
            </div>
            {!isCollapsed && (
                <div className="flex-col space-y-6">
                    {children}
                </div>
            )}
        </aside>
    );
}
