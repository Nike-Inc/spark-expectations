import React from 'react';
import './App.css';
import {useGitDetails} from "./store";
import {useUserInfo} from "./hooks";


export const Left= (): React.JSX.Element => {
    const {addToken, addBaseUrl} = useGitDetails();

    const addTokenHandler = (event: React.MouseEvent<HTMLButtonElement>) => {
        event.preventDefault();


        addToken("<YOUR_TOKEN>");
        addBaseUrl("https://api.github.com");
    }

    return (
        <>
            <button onClick={
                addTokenHandler
            }>Click Me To Get User Info
            </button>
        </>

    );
}

export const Right = (): React.JSX.Element => {
    const {token, baseUrl} = useGitDetails();

    if (!token || !baseUrl){
        return (
            <div>Enter your token and base url</div>
        )
    }

    return (
        <UserInfo/>
    );
}

const UserInfo = (): React.JSX.Element => {
    const {isPending, data, error} = useUserInfo();

    if (isPending) {
        return (
            <div>Loading...</div>
        )
    }

    if (error) {
        return (
            <div>Error: {error.message}</div>
        )
    }

    return (
        <div>
            <h1>{data?.name}</h1>
            <p>{data?.email}</p>
        </div>
    );
}


export const App = (): React.JSX.Element => {
    return (
        <>
            <Left/>  <Right/>
      </>
  );
}

