import React, { useState } from 'react';

const chevronDown = (
    <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-6 h-6 transition-transform duration-200"
    >
        <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="m19.5 8.25-7.5 7.5-7.5-7.5"
        />
    </svg>
);

const chevronUp = (
    <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-6 h-6 transition-transform duration-200 rotate-180"
    >
        <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="m19.5 8.25-7.5 7.5-7.5-7.5"
        />
    </svg>
);

export default function SidebarItem({ children, title, icon='', pulse, rotate }) {
    const [isExpanded, setIsExpanded] = useState(true);

    const toggleExpand = () => {
        setIsExpanded(!isExpanded);
    };

    return (
        <div className="w-full">
            <div
                className="flex items-center justify-between p-2 cursor-pointer text-sky-950"
                onClick={toggleExpand}
            >
                <div className="flex">
                    <div className={`${pulse ? 'animate-pulse' : ''} ${rotate ? 'animate-spin' : ''} aspect-square mx-2 w-6 text-sky-900`}>{icon}</div>
                    <span className="text-lg text-sky-950 font-medium">{title}</span>
                </div>
                {isExpanded ? chevronUp : chevronDown}
            </div>
            <div
                className={`overflow-hidden transition-all duration-300 ${
                    isExpanded ? 'max-h-screen' : 'max-h-0'
                }`}
            >
                <div className="p-2">{children}</div>
            </div>
        </div>
    );
}
