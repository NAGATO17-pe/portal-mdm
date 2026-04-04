"use client";

export const useReconnectIndicator = (isConnected: boolean): string =>
  isConnected ? "connected" : "reconnecting";
