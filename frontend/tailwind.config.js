/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      animation: {
        fadeCycle: "fadeCycle 5s infinite", // Adjust duration as needed
        fadeCycleSmooth: "fadeCycleSmooth 5s infinite",
      },
      keyframes: {
        fadeCycle: {
          "0%, 100%": { opacity: "0" },
          "10%": { opacity: "1" }, // Fades in
          "20%": { opacity: "1" }, // Fades in
          "30%": { opacity: "0.5" }, // Fades in
          "40%": {opacity: "0"},
          "50%": {opacity: "0"},
          "60%": { opacity: "0" }, // Fades out before switching
        },
        fadeCycleSmooth: {
          "0%": { opacity: "0" },
          "10%": { opacity: "0.5" }, // Fades in
          "20%": { opacity: "1" }, // Fades in
          "30%": { opacity: "1" }, // Fades in
          "40%": {opacity: "1"},
          "50%": {opacity: "0.8"},
          "60%": { opacity: "0.5" }, // Fades out before switching
          "70%": { opacity: "0" },
          "100%": {opacity: "0"}
        },
      },
    },
  },
  plugins: [],
}

