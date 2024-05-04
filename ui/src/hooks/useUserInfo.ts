import {gitApi} from "../api";
import {useQuery} from "@tanstack/react-query";


const fetchUserInfo = async () => {
    const {data} = await gitApi.get('/user');
    return data;
}

export const useUserInfo = () => {
    return useQuery({
        queryKey: ['user-info'],
        queryFn: fetchUserInfo,
        retry: 1
    })
}