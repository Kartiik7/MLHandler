export const API_BASE = import.meta.env.VITE_API_URL || 'http://api:8000';
export const WS_BASE = import.meta.env.VITE_WS_URL || API_BASE.replace('https://', 'wss://').replace('http://', 'ws://');
