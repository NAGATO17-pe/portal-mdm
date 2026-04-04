"use client";

import { useEffect, useState } from "react";

export const useReconnectIndicator = (isConnected: boolean): string => {
  const [label, setLabel] = useState("connected");

  useEffect(() => {
    setLabel(isConnected ? "connected" : "reconnecting");
  }, [isConnected]);

  return label;
};
