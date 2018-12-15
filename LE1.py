class Solution:
    def removeDuplicates(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        cnt=0
        
        if (len(nums)==0):
            return cnt 
        
        for i in sorted(set(nums)):
            nums[cnt]=i
            cnt+=1
        
        return(cnt)
