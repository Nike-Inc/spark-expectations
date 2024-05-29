import React, { FC } from 'react';
import './Loading.css'; // Import CSS for styling

interface LoadingProps {
  message?: string; // Optional prop to display a message below the spinner
}

export const Loading: FC<LoadingProps> = ({ message = 'Loading...' }) => (
  <div className="loading">
    <div className="spinner"></div>
    <p>{message}</p>
  </div>
);
