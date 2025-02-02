import { useState } from "react";

export default function FormInputBoolean({
    value = false,
    id = '',
    handleInputChange = (newValue, id) => {},
    isDisabled = false,
    inputStyles = ''
}) {
    const handleToggle = () => {
        if (!isDisabled) {
            const newValue = !value; // Toggle the boolean value
            handleInputChange(newValue, id); // Pass the updated value to the callback
        }
    };

    return (
        <div
            className={`flex items-center ${isDisabled ? 'opacity-50' : ''} ${inputStyles}`}
        >
            <span className={`mr-2 ${value ? 'text-gray-500' : 'text-black'}`}>Off</span>
            <div
                onClick={handleToggle}
                className={`relative w-12 h-6 rounded-full cursor-pointer ${
                    value ? 'bg-blue-500' : 'bg-gray-300'
                }`}
            >
                <div
                    className={`absolute top-1 left-1 w-4 h-4 bg-white rounded-full transition-transform duration-200 ${
                        value ? 'transform translate-x-6' : ''
                    }`}
                ></div>
            </div>
            <span className={`ml-2 ${value ? 'text-black' : 'text-gray-500'}`}>On</span>
        </div>
    );
}
